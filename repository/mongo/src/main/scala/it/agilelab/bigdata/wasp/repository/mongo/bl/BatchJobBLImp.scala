package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.{BatchJobBL, BatchJobInstanceBL}
import it.agilelab.bigdata.wasp.models.BatchJobModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{BatchJobDBModel, BatchJobDBModelV1}
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.mongodb.scala.bson.BsonString
import it.agilelab.bigdata.wasp.repository.core.mappers.BatchJobModelMapperSelector.factory
import it.agilelab.bigdata.wasp.repository.core.mappers.BatchJobMapperV1.transform

class BatchJobBLImp(waspDB: WaspMongoDB) extends BatchJobBL {

  val instanceBl = new BatchJobInstanceBlImp(waspDB)

  def deleteByName(name: String): Unit = {
    waspDB.deleteByName[BatchJobDBModel](name)
  }

  def getAll: Seq[BatchJobModel] = {
    waspDB.getAll[BatchJobDBModel]().map(factory)
  }

  def update(batchJobModel: BatchJobModel): Unit = {
    waspDB.updateByName[BatchJobDBModel](batchJobModel.name, transform[BatchJobDBModelV1](batchJobModel))
  }

  def insert(batchJobModel: BatchJobModel): Unit = {
    waspDB.insertIfNotExists[BatchJobDBModel](transform[BatchJobDBModelV1](batchJobModel))
  }

  def upsert(batchJobModel: BatchJobModel): Unit = {
    waspDB.upsert[BatchJobDBModel](transform[BatchJobDBModelV1](batchJobModel))
  }

  def getByName(name: String): Option[BatchJobModel] = {
    waspDB.getDocumentByField[BatchJobDBModel]("name", new BsonString(name)).map(factory)
  }

  override def instances(): BatchJobInstanceBL = instanceBl


}
