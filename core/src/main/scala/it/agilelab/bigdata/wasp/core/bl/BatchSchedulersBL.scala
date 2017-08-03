package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.BatchSchedulerModel
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.bson.BsonBoolean
import org.mongodb.scala.bson.BsonObjectId

trait BatchSchedulersBL {
  def getActiveSchedulers(isActive: Boolean = true): Seq[BatchSchedulerModel]

  def getById(id: String): Option[BatchSchedulerModel]

  def persist(schedulerModel: BatchSchedulerModel): Unit

}
class BatchSchedulersBLImp(waspDB: WaspDB) extends BatchSchedulersBL  {
  private def factory(t: BatchSchedulerModel) = new BatchSchedulerModel(t.name, t.quartzUri, t.batchJob,
                                                                        t.options, t.isActive, t._id)

  def getActiveSchedulers(isActive: Boolean = true) = {
    waspDB.getAllDocumentsByField[BatchSchedulerModel]("isActive", new BsonBoolean(isActive)).map(factory(_))
  }

  def getById(id: String) = {
    waspDB.getDocumentByID[BatchSchedulerModel](BsonObjectId(id)).map(factory)
  }

  override def persist(schedulerModel: BatchSchedulerModel): Unit = waspDB.insert[BatchSchedulerModel](schedulerModel)
}
