package it.agilelab.bigdata.wasp.repository.mongo.bl

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import it.agilelab.bigdata.wasp.repository.core.bl.BatchJobInstanceBL
import it.agilelab.bigdata.wasp.models.{BatchJobInstanceModel, JobStatus}
import it.agilelab.bigdata.wasp.repository.core.dbModels.BatchJobInstanceDBModel
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.mongodb.scala.bson.{BsonDocument, BsonInt64, BsonString}
import it.agilelab.bigdata.wasp.repository.core.mappers.BatchJobInstanceMapperV1._
import it.agilelab.bigdata.wasp.repository.core.mappers.BatchJobInstanceModelMapperSelector.applyMap

class BatchJobInstanceBlImp(waspDB: WaspMongoDB) extends BatchJobInstanceBL {
  override def update(instance: BatchJobInstanceModel): BatchJobInstanceModel = {
    waspDB.updateByName[BatchJobInstanceDBModel](instance.name, fromModelToDBModel(instance))
    instance
  }

  override def all(): Seq[BatchJobInstanceModel] =
    waspDB.getAll[BatchJobInstanceDBModel].map(applyMap)

  override def instancesOf(name: String): Seq[BatchJobInstanceModel] =
    waspDB.getAllDocumentsByField[BatchJobInstanceDBModel]("instanceOf", BsonString(name)).map(applyMap)

  override def insert(instance: BatchJobInstanceModel): BatchJobInstanceModel = {
    waspDB.insert[BatchJobInstanceDBModel](fromModelToDBModel(instance))
    instance
  }

  override def getByName(name: String): Option[BatchJobInstanceModel] = {
    waspDB.getDocumentByField[BatchJobInstanceDBModel]("name", new BsonString(name)).map(applyMap)
  }
}
