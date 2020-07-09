package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.{BatchJobBL, BatchJobInstanceBL}
import it.agilelab.bigdata.wasp.models.BatchJobModel
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.mongodb.scala.bson.BsonString

class BatchJobBLImp(waspDB: WaspMongoDB) extends BatchJobBL {

  val instanceBl = new BatchJobInstanceBlImp(waspDB)

  def deleteByName(name: String): Unit = {
    waspDB.deleteByName(name)
  }

  def getAll: Seq[BatchJobModel] = {
    waspDB.getAll[BatchJobModel]().map(factory)
  }

  def update(batchJobModel: BatchJobModel): Unit = {
    waspDB.updateByName[BatchJobModel](batchJobModel.name, batchJobModel)
  }

  def insert(batchJobModel: BatchJobModel): Unit = {
    waspDB.insertIfNotExists[BatchJobModel](batchJobModel)
  }

  def upsert(batchJobModel: BatchJobModel): Unit = {
    waspDB.upsert(batchJobModel)
  }

  def getByName(name: String): Option[BatchJobModel] = {
    waspDB.getDocumentByField[BatchJobModel]("name", new BsonString(name)).map(batchJob => {
      factory(batchJob)
    })
  }

  private def factory(p: BatchJobModel) = new BatchJobModel(p.name, p.description, p.owner, p.system, p.creationTime, p.etl, p.exclusivityConfig)

  override def instances(): BatchJobInstanceBL = instanceBl


}
