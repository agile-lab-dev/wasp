package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

import java.util.UUID

import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.Data.Collaborator
import it.agilelab.bigdata.wasp.models.PipegraphStatus.PipegraphStatus
import it.agilelab.bigdata.wasp.models.{PipegraphInstanceModel, PipegraphModel, PipegraphStatus}
import org.apache.commons.lang3.exception.ExceptionUtils

import scala.util.{Failure, Success, Try}

/**
  * Trait holding the Database operations needed by [[SparkConsumersStreamingMasterGuardian]]
  */
private[streaming] trait DatabaseOperations {
  this: SparkConsumersStreamingMasterGuardian =>

  /**
    * Preprocessing method that checks that currently not supported components are not used.
    *
    * @param model The pipegraph model to check
    * @return The checked pipegraphmodel
    */
  def checkThatModelDoesNotContainLegacyOrRTComponents(model: PipegraphModel): Try[PipegraphModel] = {

    val errors = Seq(
      (() => model.legacyStreamingComponents.nonEmpty, "No legacy streaming etl model allowed in pipegraph definition"),
      (() => model.rtComponents.nonEmpty, "No rt etl model allowed in pipegraph definition")
    ).filter(_._1())
      .map(_._2)

    if (errors.nonEmpty) {
      Failure(new Exception(errors.mkString(",")))
    } else {
      Success(model)
    }

  }

  def createInstanceOf(modelName: String): Try[PipegraphInstanceModel] =
    for {
      model <- retrievePipegraph(modelName).flatMap(checkThatModelDoesNotContainLegacyOrRTComponents)
      instance <- createInstanceOf(model)
    } yield instance

  /**
    * Creates an instance of the specified model.
    *
    * @param model The model to instantiate
    * @return A Try containing the instance model or an exception
    */
  def createInstanceOf(model: PipegraphModel): Try[PipegraphInstanceModel] = Try {

    val instance = PipegraphInstanceModel(
      name = s"${model.name}-${UUID.randomUUID().toString}",
      instanceOf = model.name,
      startTimestamp = System.currentTimeMillis(),
      currentStatusTimestamp = -1,
      status = PipegraphStatus.PENDING,
      executedByNode = None,
      peerActor = None
    )

    pipegraphBL.instances().insert(instance)
  }

  def resetStatesWhileRecoveringAndReturnPending(knownMembers: Set[Collaborator]): Try[(Seq[(PipegraphModel, PipegraphInstanceModel)], Seq[(PipegraphModel, PipegraphInstanceModel)])] = {
    for {

      pendingOnAllNodes <- retrievePipegraphInstancesAndPipegraphs(PipegraphStatus.PENDING)
      processingOnAllNodes <- retrievePipegraphInstancesAndPipegraphs(PipegraphStatus.PROCESSING)
      stoppingOnAllNodes <- retrievePipegraphInstancesAndPipegraphs(PipegraphStatus.STOPPING)
      unschedulable <- retrievePipegraphInstancesAndPipegraphs(PipegraphStatus.UNSCHEDULABLE)


      needsRecovering = (pendingOnAllNodes.toList ::: processingOnAllNodes.toList).filterNot {
        case (_, instance) =>
          knownMembers
            .map(m => SparkConsumersStreamingMasterGuardian.formatUniqueAddress(m.address))
            .contains(instance.executedByNode.getOrElse(""))
      }


      processingOnThisNode = processingOnAllNodes.filter {
        case (_, instance) =>
          instance.executedByNode.getOrElse("") == SparkConsumersStreamingMasterGuardian.formatUniqueAddress(cluster.selfUniqueAddress)
      }

      _ <- updateToStatus(stoppingOnAllNodes, PipegraphStatus.STOPPED)
      updated <- updateToStatus(needsRecovering ++ unschedulable, PipegraphStatus.PENDING)


    } yield (processingOnThisNode, updated)
  }

  /**
    * Retrieves [[PipegraphInstanceModel]]s with specified statuses
    *
    * @param status the status of the [[PipegraphInstanceModel]] that should be retrieved
    * @return A Try containing the [[PipegraphInstanceModel]]s or an exception
    */
  def retrievePipegraphInstances(status: PipegraphStatus*): Try[Seq[PipegraphInstanceModel]] = Try {
    pipegraphBL.instances().all().filter { model =>
      status.toSet.contains(model.status)
    }
  }

  def retrievePipegraphInstancesAndPipegraphs(status: PipegraphStatus*): Try[Seq[(PipegraphModel, PipegraphInstanceModel)]] = Try {
    val instances = pipegraphBL.instances().all().filter { model =>
      status.toSet.contains(model.status)
    }

    instances.map { instance =>
      val pipegraph = pipegraphBL.getByName(instance.instanceOf).get
      (pipegraph, instance)
    }
  }


  /**
    * Updates the status of the supplied instances to the target status.
    *
    * @param pipegraphInstances The instances to update
    * @param targetStatus       The target status
    * @return A try containing the updated [[PipegraphInstanceModel]] or an exception.
    */
  def updateToStatus(
                      pipegraphInstances: Seq[(PipegraphModel, PipegraphInstanceModel)],
                      targetStatus: PipegraphStatus
                    ): Try[Seq[(PipegraphModel, PipegraphInstanceModel)]] = {
    val preprocess = if (targetStatus == PipegraphStatus.PENDING) { (model: PipegraphInstanceModel) =>
      model.copy(executedByNode = None, peerActor = None)
    } else { (model: PipegraphInstanceModel) =>
      model
    }

    val result = pipegraphInstances.map {
      case (pipegraph, instance) => (pipegraph, preprocess(instance))
    }.map {
      case (pipegraph, instance) =>
        updateToStatus(instance, targetStatus).map { updatedInstance =>
          (pipegraph, updatedInstance)
        }
    }

    val exceptions = result
      .map {
        case Failure(ex) => Some(ex)
        case Success(_) => None
      }
      .filter(_.isDefined)
      .map(_.get)

    val models = result
      .map {
        case Failure(_) => None
        case Success(model) => Some(model)
      }
      .filter(_.isDefined)
      .map(_.get)

    if (exceptions.nonEmpty) {
      Failure(exceptions.head)
    } else {
      Success(models)
    }

  }

  def retrievePipegraphAndUpdateInstanceToProcessing(
                                                      pipegraphInstanceModel: PipegraphInstanceModel
                                                    ): Try[(PipegraphModel, PipegraphInstanceModel)] =
    for {
      model <- retrievePipegraph(pipegraphInstanceModel.instanceOf)
      instance <- updateToStatus(pipegraphInstanceModel, PipegraphStatus.PROCESSING)
    } yield (model, instance)

  /**
    * Retrieves [[PipegraphModel]]s with specified name from the db.
    *
    * if no [[PipegraphModel]] exist with name failure is returned
    *
    * @return A Try containing the [[PipegraphModel]]s or an exception
    */
  def retrievePipegraph(name: String): Try[PipegraphModel] =
    Try {
      pipegraphBL.getByName(name)
    }.flatMap {
      case Some(result) => Success(result)
      case None => Failure(new Exception("Could not retrieve batchJob"))
    }

  /**
    * Retrieves [[PipegraphModel]]s that are marked as system
    *
    * @return A Try containing the [[PipegraphModel]]s or an exception
    */
  def retrieveSystemPipegraphs(): Try[Seq[PipegraphModel]] = Try {
    pipegraphBL.getSystemPipegraphs
  }

  /**
    * Updates the status of the supplied instance to the target status.
    *
    * @param jobInstance  The instance to update
    * @param targetStatus The target status
    * @param maybeError   Maybe an error to persist on the database
    * @return A try containing the updated [[PipegraphInstanceModel]] or an exception.
    */
  def updateToStatus(
                      jobInstance: PipegraphInstanceModel,
                      targetStatus: PipegraphStatus,
                      maybeError: Option[Throwable] = None
                    ) = Try {

    val updated = maybeError
      .map { error =>
        ExceptionUtils.getStackTrace(error)
      }
      .map { error =>
        jobInstance.copy(error = Some(error))
      }
      .getOrElse(jobInstance)
      .copy(status = targetStatus, currentStatusTimestamp = System.currentTimeMillis())

    pipegraphBL.instances().update(updated)
  }
}
