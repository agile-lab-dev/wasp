package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.models.HttpModel
import it.agilelab.bigdata.wasp.repository.core.bl.HttpBL
import it.agilelab.bigdata.wasp.repository.core.dbModels.{HttpDBModel, HttpDBModelV1}
import it.agilelab.bigdata.wasp.repository.core.mappers.HttpDBModelMapperSelector.factory
import it.agilelab.bigdata.wasp.repository.core.mappers.HttpMapperV1.transform
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.mongodb.scala.bson.BsonString

case class HttpBlImpl(waspDB: WaspMongoDB) extends HttpBL {


  def getByName(name: String): Option[HttpModel] = {
    waspDB
      .getDocumentByField[HttpDBModel]("name", new BsonString(name))
      .map(factory)
  }

  override def persist(HttpModel: HttpModel): Unit =
    waspDB.insert[HttpDBModel](transform[HttpDBModelV1](HttpModel))

  override def insertIfNotExists(HttpModel: HttpModel): Unit =
    waspDB.insertIfNotExists[HttpDBModel](transform[HttpDBModelV1](HttpModel))

  override def upsert(HttpModel: HttpModel): Unit =
    waspDB.upsert[HttpDBModel](transform[HttpDBModelV1](HttpModel))

  override def getAll(): Seq[HttpModel] =
    waspDB.getAll[HttpDBModel]().map(factory)
}
