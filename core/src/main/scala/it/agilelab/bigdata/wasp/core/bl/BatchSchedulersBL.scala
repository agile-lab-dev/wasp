package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.{BSONConversionHelper, BatchSchedulerModel}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import reactivemongo.bson.{BSONBoolean, BSONObjectID}
import reactivemongo.api.commands.WriteResult

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait BatchSchedulersBL {
  def getActiveSchedulers(isActive: Boolean = true): Future[List[BatchSchedulerModel]]

  def getById(id: String): Future[Option[BatchSchedulerModel]]

  def persist(schedulerModel: BatchSchedulerModel): Future[WriteResult]

}
class BatchSchedulersBLImp(waspDB: WaspDB) extends BatchSchedulersBL with BSONConversionHelper {
  private def factory(t: BatchSchedulerModel) = new BatchSchedulerModel(t.name, t.quartzUri, t.batchJob,
                                                                        t.options, t.isActive, t._id)

  def getActiveSchedulers(isActive: Boolean = true) = {
    waspDB.getAllDocumentsByField[BatchSchedulerModel]("isActive", new BSONBoolean(isActive)).map(scheduler => {
      scheduler.map(p => factory(p))
    })
  }

  def getById(id: String) = {
    waspDB.getDocumentByID[BatchSchedulerModel](BSONObjectID(id)).map(scheduler => {
      scheduler.map(p => factory(p))
    })
  }

  override def persist(schedulerModel: BatchSchedulerModel): Future[WriteResult] = waspDB.insert[BatchSchedulerModel](schedulerModel)
}
