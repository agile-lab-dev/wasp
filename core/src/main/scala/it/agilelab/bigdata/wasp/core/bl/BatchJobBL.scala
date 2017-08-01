package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.{BSONConversionHelper, BatchJobModel, JobStateEnum}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import reactivemongo.bson.{BSONObjectID, BSONString}
import reactivemongo.api.commands.WriteResult

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait BatchJobBL {


  def getPendingJobs (state: String = JobStateEnum.PENDING): Future[List[BatchJobModel]]

  def getById(id: String):  Future[Option[BatchJobModel]]

  def getAll: Future[List[BatchJobModel]]

  def update(batchJobModel: BatchJobModel): Future[WriteResult]

  def setJobState(batchJobModel: BatchJobModel, newState: String) = {
    newState match {
      case JobStateEnum.SUCCESSFUL | JobStateEnum.FAILED | JobStateEnum.PENDING | JobStateEnum.PROCESSING =>
        batchJobModel.state = newState
        update(batchJobModel)
    }
  }

  def insert(batchJobModel: BatchJobModel): Future[WriteResult]

  def persist(batchJobModel: BatchJobModel): Future[WriteResult]

  def deleteById(id_string: String): Future[WriteResult]


}

class BatchJobBLImp(waspDB: WaspDB) extends BatchJobBL with BSONConversionHelper {

  private def factory(p: BatchJobModel) = new BatchJobModel(p.name, p.description, p.owner, p.system, p.creationTime, p.etl, p.state, p._id)

  def getPendingJobs (state: String = JobStateEnum.PENDING): Future[List[BatchJobModel]] = {
    waspDB.getAllDocumentsByField[BatchJobModel]("state",  new BSONString(state) ).map(job => {
      job.map(p => factory(p))
    })
  }

  def getById(id: String): Future[Option[BatchJobModel]] = {
    waspDB.getDocumentByID[BatchJobModel](BSONObjectID(id)).map(batchJob => {
      batchJob.map(p => factory(p))
    })
  }

  def getAll: Future[List[BatchJobModel]] = {
    waspDB.getAll[BatchJobModel].map(producer => {
      producer.map(p => factory(p))
    })
  }

  def update(batchJobModel: BatchJobModel): Future[WriteResult] = {
    waspDB.updateById[BatchJobModel](batchJobModel._id.get, batchJobModel)
  }

  def insert(batchJobModel : BatchJobModel): Future[WriteResult] =
  {
    waspDB.insertIfNotExists[BatchJobModel](batchJobModel)
  }

  override def persist(batchJobModel: BatchJobModel): Future[WriteResult] = {
    waspDB.insert[BatchJobModel](batchJobModel)
  }

  def deleteById(id_string: String): Future[WriteResult] = {
    waspDB.deleteById[BatchJobModel](BSONObjectID(id_string))
  }
}

