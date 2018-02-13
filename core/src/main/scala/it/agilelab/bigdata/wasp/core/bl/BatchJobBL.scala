package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.{BatchJobModel, JobStateEnum}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.bson.BsonString
import org.mongodb.scala.bson.BsonObjectId

trait BatchJobBL {

  def getPendingJobs (state: String = JobStateEnum.PENDING): Seq[BatchJobModel]

  def getByName(name: String): Option[BatchJobModel]

  def getAll: Seq[BatchJobModel]

  def update(batchJobModel: BatchJobModel): Unit

  def setJobState(batchJobModel: BatchJobModel, newState: String) = {
    newState match {
      case JobStateEnum.SUCCESSFUL | JobStateEnum.FAILED | JobStateEnum.PENDING | JobStateEnum.PROCESSING =>
        batchJobModel.state = newState
        update(batchJobModel)
    }
  }

  def insert(batchJobModel: BatchJobModel): Unit

  def persist(batchJobModel: BatchJobModel): Unit

  def deleteByName(name: String): Unit
}

class BatchJobBLImp(waspDB: WaspDB) extends BatchJobBL {

  private def factory(p: BatchJobModel) = new BatchJobModel(p.name, p.description, p.owner, p.system, p.creationTime, p.etl, p.state)

  def getPendingJobs (state: String = JobStateEnum.PENDING): Seq[BatchJobModel] = {
    waspDB.getAllDocumentsByField[BatchJobModel]("state",  new BsonString(state)).map(job => {
      factory(job)
    })
  }

  def deleteByName(name: String): Unit = {
    waspDB.deleteByName(name)
  }

  def getAll: Seq[BatchJobModel] = {
    waspDB.getAll[BatchJobModel]().map( factory)
  }

  def update(batchJobModel: BatchJobModel): Unit = {
    waspDB.updateByName[BatchJobModel](batchJobModel.name, batchJobModel)
  }

  def insert(batchJobModel : BatchJobModel): Unit =
  {
    waspDB.insertIfNotExists[BatchJobModel](batchJobModel)
  }

  override def persist(batchJobModel: BatchJobModel): Unit = {
    waspDB.insert[BatchJobModel](batchJobModel)
  }


  def getByName(name: String): Option[BatchJobModel] = {
    waspDB.getDocumentByField[BatchJobModel]("name", new BsonString(name)).map(batchJob => {
      factory(batchJob)
    })
  }
}