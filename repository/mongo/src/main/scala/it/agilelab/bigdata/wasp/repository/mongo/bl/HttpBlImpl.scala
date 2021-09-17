package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.models.{HttpModel, HttpCompression}
import it.agilelab.bigdata.wasp.repository.core.bl.HttpBL
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
      .getDocumentByFieldRaw[HttpModel]("name", new BsonString(name))
      .map(factory)
  }

  override def persist(HttpModel: HttpModel): Unit =
    waspDB.insert[HttpModel](HttpModel)

  override def insertIfNotExists(HttpModel: HttpModel): Unit =
    waspDB.insertIfNotExists[HttpModel](HttpModel)

  override def upsert(HttpModel: HttpModel): Unit =
    waspDB.upsert[HttpModel](HttpModel)

  override def getAll(): Seq[HttpModel] = waspDB.getAllRaw[HttpModel]().map(factory)
}
