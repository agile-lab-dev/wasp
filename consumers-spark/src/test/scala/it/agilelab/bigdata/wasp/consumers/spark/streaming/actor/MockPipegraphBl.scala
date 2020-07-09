package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor

import it.agilelab.bigdata.wasp.repository.core.bl.{PipegraphBL, PipegraphInstanceBl}
import it.agilelab.bigdata.wasp.models.{PipegraphModel, PipegraphStatus}
import it.agilelab.bigdata.wasp.models.PipegraphStatus.PipegraphStatus

import scala.collection.mutable.ListBuffer

class MockPipegraphBl(instanceBl: PipegraphInstanceBl) extends PipegraphBL {

  val buffer: ListBuffer[PipegraphModel] = ListBuffer()

  override def getAll: Seq[PipegraphModel] =
    buffer.clone()

  override def getSystemPipegraphs: Seq[PipegraphModel] =
    buffer.filter(_.isSystem)

  override def getNonSystemPipegraphs: Seq[PipegraphModel] =
    buffer.filterNot(_.isSystem)

  override def getActivePipegraphs(): Seq[PipegraphModel] = {

    val allowedStates: Set[PipegraphStatus] = Set(PipegraphStatus.PENDING, PipegraphStatus.PROCESSING)


    instances().all()
      .filter(instance => allowedStates.contains(instance.status))
      .flatMap(instance => getByName(instance.name))

  }

  override def getByName(name: String): Option[PipegraphModel] =
    buffer.find(_.name == name)

  override def instances(): PipegraphInstanceBl = instanceBl

  override def insert(pipegraph: PipegraphModel): Unit =
    buffer += pipegraph

  override def update(pipegraphModel: PipegraphModel): Unit = {
    buffer.remove(buffer.toIndexedSeq.indexWhere(_.name == pipegraphModel.name))
    buffer += pipegraphModel
  }

  override def deleteByName(name: String): Unit = {
    buffer.remove(buffer.toIndexedSeq.indexWhere(_.name == name))
  }

  override def upsert(pipegraph: PipegraphModel): Unit =
    if(getByName(pipegraph.name).isDefined) update(pipegraph)
    else insert(pipegraph)
}
