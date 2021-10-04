package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.models.{HttpCompression, HttpModel}
import it.agilelab.bigdata.wasp.repository.core.bl.HttpBL
import it.agilelab.bigdata.wasp.repository.core.dbModels.HttpDBModel
import it.agilelab.bigdata.wasp.repository.core.mappers.HttpDBModelMapperSelector.applyMap
import it.agilelab.bigdata.wasp.repository.core.mappers.HttpMapperV1.fromModelToDBModel
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.mongodb.scala.bson.{BsonDocument, BsonString}

import scala.collection.JavaConverters._

case class HttpBlImpl(waspDB: WaspMongoDB) extends HttpBL {

  private def factory(t: BsonDocument): HttpModel =
    HttpModel(
      t.get("name").asString().getValue,
      t.get("url").asString().getValue,
      t.get("method").asString().getValue,
      Option(t.get("headersFieldName")).map(_.asString().getValue),
      t.getArray("valueFieldsNames").getValues.asScala.toList.map(_.asString().getValue),
      HttpCompression.fromString(t.get("compression").asString().getValue),
      t.get("mediaType").asString().getValue,
      t.get("logBody").asBoolean().getValue,
      t.get("structured").asBoolean().getValue
    )

  def getByName(name: String): Option[HttpModel] = {
    waspDB
      .getDocumentByField[HttpDBModel]("name", new BsonString(name))
      .map(applyMap)
  }

  override def persist(HttpModel: HttpModel): Unit =
    waspDB.insert[HttpDBModel](fromModelToDBModel(HttpModel))

  override def insertIfNotExists(HttpModel: HttpModel): Unit =
    waspDB.insertIfNotExists[HttpDBModel](fromModelToDBModel(HttpModel))

  override def upsert(HttpModel: HttpModel): Unit =
    waspDB.upsert[HttpDBModel](fromModelToDBModel(HttpModel))

  override def getAll(): Seq[HttpModel] =
    waspDB.getAll[HttpDBModel]().map(applyMap)
}
