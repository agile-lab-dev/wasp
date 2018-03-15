package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.guardian.master

import java.util.UUID

import it.agilelab.bigdata.wasp.core.models.PipegraphStatus.PipegraphStatus
import it.agilelab.bigdata.wasp.core.models.{PipegraphInstanceModel, PipegraphModel, PipegraphStatus}
import org.apache.commons.lang3.exception.ExceptionUtils

import scala.util.{Failure, Success, Try}

private[streaming] trait DatabaseOperations {
  this: SparkConsumersStreamingMasterGuardian =>



  def createInstanceOf(modelName: String): Try[PipegraphInstanceModel] = for {
    model <- retrievePipegraph(modelName)
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
      status = PipegraphStatus.PENDING
    )

    pipegraphBL.instances().insert(instance)
  }

  def resetStatesWhileRecoveringAndReturnPending: Try[Seq[PipegraphInstanceModel]] = for {
    needsRecovering <- retrievePipegraphInstances(PipegraphStatus.PENDING,
      PipegraphStatus.PROCESSING,
      PipegraphStatus.STOPPING)
    stopping = needsRecovering.filter(_.status == PipegraphStatus.STOPPING)
    notStopping = needsRecovering.filterNot(_.status == PipegraphStatus.STOPPING)
    _ <- updateToStatus(stopping, PipegraphStatus.STOPPED)
    updated <- updateToStatus(notStopping, PipegraphStatus.PENDING)
  } yield updated

  /**
    * Retrieves [[PipegraphInstanceModel]]s with specified statuses
    *
    * @param status the status of the [[PipegraphInstanceModel]] that should be retrieved
    * @return A Try containing the [[PipegraphInstanceModel]]s or an exception
    */
  def retrievePipegraphInstances(status: PipegraphStatus*): Try[Seq[PipegraphInstanceModel]] = Try {
    pipegraphBL.instances().all().filter { model => status.toSet.contains(model.status) }
  }

  /**
    * Updates the status of the supplied instances to the target status.
    *
    * @param pipegraphInstances The instances to update
    * @param targetStatus       The target status
    * @return A try containing the updated [[PipegraphInstanceModel]] or an exception.
    */
  def updateToStatus(pipegraphInstances: Seq[PipegraphInstanceModel], targetStatus: PipegraphStatus)
  : Try[Seq[PipegraphInstanceModel]] = {
    val result = pipegraphInstances.map(updateToStatus(_, targetStatus))


    val exceptions = result.map {
      case Failure(ex) => Some(ex)
      case Success(_) => None
    }.filter(_.isDefined).map(_.get)

    val models = result.map {
      case Failure(_) => None
      case Success(model) => Some(model)
    }.filter(_.isDefined).map(_.get)


    if (exceptions.nonEmpty) {
      Failure(exceptions.head)
    } else {
      Success(models)
    }

  }

  def retrievePipegraphAndUpdateInstanceToProcessing(pipegraphInstanceModel: PipegraphInstanceModel): Try[
    (PipegraphModel, PipegraphInstanceModel)] = for {
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
  def retrievePipegraph(name: String): Try[PipegraphModel] = Try {
    pipegraphBL.getByName(name)
  }.flatMap {
    case Some(result) => Success(result)
    case None => Failure(new Exception("Could not retrieve batchJob"))
  }

  /**
    * Updates the status of the supplied instance to the target status.
    *
    * @param jobInstance  The instance to update
    * @param targetStatus The target status
    * @param maybeError   Maybe an error to persist on the database
    * @return A try containing the updated [[PipegraphInstanceModel]] or an exception.
    */
  def updateToStatus(jobInstance: PipegraphInstanceModel, targetStatus: PipegraphStatus, maybeError: Option[Throwable] =
  None) = Try {

    val updated = maybeError.map { error => ExceptionUtils.getStackTrace(error) }
      .map { error => jobInstance.copy(error = Some(error)) }
      .getOrElse(jobInstance)
      .copy(status = targetStatus, currentStatusTimestamp = System.currentTimeMillis())

    pipegraphBL.instances().update(updated)
  }
}
