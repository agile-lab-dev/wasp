package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.{BatchJobBL, BatchJobInstanceBL}
import it.agilelab.bigdata.wasp.models.BatchJobModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.BatchJobDBModel
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.mongodb.scala.bson.BsonString
import it.agilelab.bigdata.wasp.repository.core.mappers.BatchJobModelMapperSelector.applyMap
import it.agilelab.bigdata.wasp.repository.core.mappers.BatchJobMapperV1.fromModelToDBModel

class BatchJobBLImp(waspDB: WaspMongoDB) extends BatchJobBL {

  val instanceBl = new BatchJobInstanceBlImp(waspDB)

  def deleteByName(name: String): Unit = {
    waspDB.deleteByName[BatchJobDBModel](name)
  }

  def getAll: Seq[BatchJobModel] = {
    waspDB.getAll[BatchJobDBModel]().map(applyMap)
  }

  def update(batchJobModel: BatchJobModel): Unit = {
    waspDB.updateByName[BatchJobDBModel](batchJobModel.name, fromModelToDBModel(batchJobModel))
  }

  def insert(batchJobModel: BatchJobModel): Unit = {
    waspDB.insertIfNotExists[BatchJobDBModel](fromModelToDBModel(batchJobModel))
  }

  def upsert(batchJobModel: BatchJobModel): Unit = {
    waspDB.upsert[BatchJobDBModel](fromModelToDBModel(batchJobModel))
  }

  def getByName(name: String): Option[BatchJobModel] = {
    waspDB.getDocumentByField[BatchJobDBModel]("name", new BsonString(name)).map(applyMap)
  }

  override def instances(): BatchJobInstanceBL = instanceBl


}
