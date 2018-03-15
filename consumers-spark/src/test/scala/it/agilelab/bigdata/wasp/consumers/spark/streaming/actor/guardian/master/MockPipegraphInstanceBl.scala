package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.guardian.master

import it.agilelab.bigdata.wasp.core.bl.PipegraphInstanceBl
import it.agilelab.bigdata.wasp.core.models.PipegraphInstanceModel

import scala.collection.mutable.ListBuffer

class MockPipegraphInstanceBl extends PipegraphInstanceBl {

  val buffer: ListBuffer[PipegraphInstanceModel] = ListBuffer()

  override def insert(instance: PipegraphInstanceModel): PipegraphInstanceModel = {
    buffer += instance
    instance
  }

  override def update(instance: PipegraphInstanceModel): PipegraphInstanceModel = {
    buffer(buffer.toIndexedSeq.indexWhere(_.name == instance.name)) = instance
    instance
  }

  override def all(): Seq[PipegraphInstanceModel] = buffer

  override def instancesOf(name: String): Seq[PipegraphInstanceModel] = buffer.filter(_.instanceOf == name)
}
