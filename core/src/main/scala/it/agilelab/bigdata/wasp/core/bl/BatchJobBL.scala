package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.{BatchJobModel, JobStateEnum}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.bson.BsonString
import org.mongodb.scala.bson.BsonObjectId

trait BatchJobBL {

  def getPendingJobs (state: String = JobStateEnum.PENDING): Seq[BatchJobModel]

  def getById(id: String):  Option[BatchJobModel]

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

  def deleteById(id_string: String): Unit

  def deleteByName(name: String): Unit
}

class BatchJobBLImp(waspDB: WaspDB) extends BatchJobBL {

  private def factory(p: BatchJobModel) = new BatchJobModel(p.name, p.description, p.owner, p.system, p.creationTime, p.etl, p.state, p._id)

  def getPendingJobs (state: String = JobStateEnum.PENDING): Seq[BatchJobModel] = {
    waspDB.getAllDocumentsByField[BatchJobModel]("state",  new BsonString(state)).map(job => {
      factory(job)
    })
  }

  def getById(id: String): Option[BatchJobModel] = {
    waspDB.getDocumentByID[BatchJobModel](BsonObjectId(id)).map(batchJob => {
      factory(batchJob)
    })
  }

  def deleteByName(name: String): Unit = {
    val batchJobBL = getByName(name)
    if (batchJobBL.isDefined) {
      waspDB.deleteById[BatchJobModel](batchJobBL.get._id.get)
    } else {
      throw new Exception(s"Batch Job with name $name NOT FOUND")
    }
  }

  def getAll: Seq[BatchJobModel] = {
    waspDB.getAll[BatchJobModel]().map( factory)
  }

  def update(batchJobModel: BatchJobModel): Unit = {
    waspDB.updateById[BatchJobModel](batchJobModel._id.get, batchJobModel)
  }

  def insert(batchJobModel : BatchJobModel): Unit =
  {
    waspDB.insertIfNotExists[BatchJobModel](batchJobModel)
  }

  override def persist(batchJobModel: BatchJobModel): Unit = {
    waspDB.insert[BatchJobModel](batchJobModel)
  }

  def deleteById(id_string: String): Unit = {
    waspDB.deleteById[BatchJobModel](BsonObjectId(id_string))
  }

  def getByName(name: String): Option[BatchJobModel] = {
    waspDB.getDocumentByField[BatchJobModel]("name", new BsonString(name)).map(batchJob => {
      factory(batchJob)
    })
  }
}