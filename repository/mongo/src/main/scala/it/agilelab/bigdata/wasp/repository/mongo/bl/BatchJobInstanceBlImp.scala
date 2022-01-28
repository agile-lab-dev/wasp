package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.BatchJobInstanceBL
import it.agilelab.bigdata.wasp.models.BatchJobInstanceModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{BatchJobInstanceDBModel, BatchJobInstanceDBModelV1}
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.mongodb.scala.bson.BsonString
import it.agilelab.bigdata.wasp.repository.core.mappers.BatchJobInstanceMapperV1.transform
import it.agilelab.bigdata.wasp.repository.core.mappers.BatchJobInstanceModelMapperSelector.factory

class BatchJobInstanceBlImp(waspDB: WaspMongoDB) extends BatchJobInstanceBL {
  override def update(instance: BatchJobInstanceModel): BatchJobInstanceModel = {
    waspDB.updateByName[BatchJobInstanceDBModel](instance.name, transform[BatchJobInstanceDBModelV1](instance))
    instance
  }

  override def all(): Seq[BatchJobInstanceModel] =
    waspDB.getAll[BatchJobInstanceDBModel].map(factory)

  override def instancesOf(name: String): Seq[BatchJobInstanceModel] =
    waspDB.getAllDocumentsByField[BatchJobInstanceDBModel]("instanceOf", BsonString(name)).map(factory)

  override def insert(instance: BatchJobInstanceModel): BatchJobInstanceModel = {
    waspDB.insert[BatchJobInstanceDBModel](transform[BatchJobInstanceDBModelV1](instance))
    instance
  }

  override def getByName(name: String): Option[BatchJobInstanceModel] = {
    waspDB.getDocumentByField[BatchJobInstanceDBModel]("name", new BsonString(name)).map(factory)
  }
}
