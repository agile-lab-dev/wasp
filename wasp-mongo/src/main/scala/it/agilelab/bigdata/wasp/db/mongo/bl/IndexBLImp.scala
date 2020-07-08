package it.agilelab.bigdata.wasp.db.mongo.bl

import it.agilelab.bigdata.wasp.core.bl.IndexBL
import it.agilelab.bigdata.wasp.core.models.IndexModel
import it.agilelab.bigdata.wasp.db.mongo.WaspMongoDB
import org.mongodb.scala.bson.{BsonDocument, BsonString}

class IndexBLImp(waspDB: WaspMongoDB) extends IndexBL {

  private def factory(t: BsonDocument): IndexModel =
    IndexModel(
      t.get("name").asString().getValue,t.get("creationTime").asInt64().getValue,
      Option(t.get("schema")).map(_.asString().getValue),
      Option(t.get("query")).map(_.asString().getValue),
      Option(t.get("numShards")).map(_.asInt32().getValue),
      Option(t.get("replicationFactor")).map(_.asInt32().getValue),
      t.get("rollingIndex").asBoolean().getValue,
      Option(t.get("idField")).map(_.asString().getValue)
    )

  def getByName(name: String): Option[IndexModel] = {
    waspDB
      .getDocumentByFieldRaw[IndexModel]("name", new BsonString(name))
      .map(factory)
  }

  override def persist(indexModel: IndexModel): Unit =
    waspDB.insert[IndexModel](indexModel)

  override def insertIfNotExists(indexModel: IndexModel): Unit =
    waspDB.insertIfNotExists[IndexModel](indexModel)

  override def upsert(indexModel: IndexModel): Unit =
    waspDB.insert[IndexModel](indexModel)

  override def getAll(): Seq[IndexModel] = waspDB.getAllRaw[IndexModel]().map(factory)
}
