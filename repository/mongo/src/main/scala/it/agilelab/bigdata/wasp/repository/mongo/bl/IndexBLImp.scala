package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.IndexBL
import it.agilelab.bigdata.wasp.models.IndexModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.IndexDBModel
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.mongodb.scala.bson.{BsonDocument, BsonString}
import it.agilelab.bigdata.wasp.repository.core.mappers.IndexDBModelMapperSelector.applyMap
import it.agilelab.bigdata.wasp.repository.core.mappers.IndexMapperV1.fromModelToDBModel

class IndexBLImp(waspDB: WaspMongoDB) extends IndexBL {

//  private def factory(t: BsonDocument): IndexModel =
//    IndexModel(
//      t.get("name").asString().getValue,t.get("creationTime").asInt64().getValue,
//      Option(t.get("schema")).map(_.asString().getValue),
//      Option(t.get("query")).map(_.asString().getValue),
//      Option(t.get("numShards")).map(_.asInt32().getValue),
//      Option(t.get("replicationFactor")).map(_.asInt32().getValue),
//      t.get("rollingIndex").asBoolean().getValue,
//      Option(t.get("idField")).map(_.asString().getValue)
//    )

  def getByName(name: String): Option[IndexModel] = {
    waspDB
      .getDocumentByField[IndexDBModel]("name", new BsonString(name))
      .map(applyMap)
  }

  override def persist(indexModel: IndexModel): Unit =
    waspDB.insert[IndexDBModel](fromModelToDBModel(indexModel))

  override def insertIfNotExists(indexModel: IndexModel): Unit =
    waspDB.insertIfNotExists[IndexDBModel](fromModelToDBModel(indexModel))

  override def upsert(indexModel: IndexModel): Unit =
    waspDB.upsert[IndexDBModel](fromModelToDBModel(indexModel)) // changed bug?

  override def getAll(): Seq[IndexModel] = waspDB.getAll[IndexDBModel]().map(applyMap)
}
