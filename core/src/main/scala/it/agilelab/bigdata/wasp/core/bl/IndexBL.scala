package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.{IndexModel, TopicModel}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.mongodb.scala.bson.{BsonDocument, BsonObjectId, BsonString}

import scala.concurrent.Future

trait IndexBL {
  def getByName(name: String): Option[IndexModel]

  def getById(id: String): Option[IndexModel]

  def persist(indexModel: IndexModel): Unit

}

class IndexBLImp(waspDB: WaspDB) extends IndexBL {

  private def factory(t: BsonDocument): IndexModel =
    new IndexModel(t.get("name").asString().getValue, t.get("creationTime").asInt64().getValue, Option(t.get("schema").asDocument()),
      Some(t.get("_id").asObjectId()), Option(t.get("query")).map(_.asString().getValue),
      Option(t.get("numShards")).map(_.asInt32().getValue), Option(t.get("replicationFactor")).map(_.asInt32().getValue))

  def getByName(name: String) = {
    waspDB
      .getDocumentByFieldRaw[IndexModel]("name", new BsonString(name))
      .map(factory)
  }

  def getById(id: String) = {
    waspDB
      .getDocumentByIDRaw[IndexModel](BsonObjectId(id))
      .map(factory)
  }

  override def persist(indexModel: IndexModel): Unit =
    waspDB.insert[IndexModel](indexModel)
}
