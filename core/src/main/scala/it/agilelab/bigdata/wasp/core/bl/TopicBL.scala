package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.TopicModel
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.bson.BsonString
import org.mongodb.scala.bson.{BsonDocument, BsonObjectId}

import scala.concurrent.Future

trait TopicBL {

  def getByName(name: String): Option[TopicModel]

  def getById(id: String): Option[TopicModel]

  def getAll : Seq[TopicModel]

  def persist(topicModel: TopicModel): Unit

}

class TopicBLImp(waspDB: WaspDB) extends TopicBL  {

  private def factory(t: BsonDocument): TopicModel = new TopicModel(t.get("name").asString().getValue, t.get("creationTime").asInt64().getValue,
      t.get("partitions").asInt32().getValue, t.get("replicas").asInt32().getValue,
    t.get("topicDataType").asString().getValue , Option(t.getOrDefault("partitionKeyField", BsonDocument()).asString().getValue), Option(t.get("schema").asDocument()), Some(t.get("_id").asObjectId()))

  def getByName(name: String): Option[TopicModel] = {
    waspDB.getDocumentByFieldRaw[TopicModel]("name", new BsonString(name)).map(topic => {
      factory(topic)
    })
  }

  def getById(id: String): Option[TopicModel] = {
    waspDB.getDocumentByIDRaw[TopicModel](BsonObjectId(id)).map(topic => {
      factory(topic)
    })
  }

  def getAll: Seq[TopicModel] = {
    waspDB.getAllRaw[TopicModel]().map(factory)
  }

  override def persist(topicModel: TopicModel): Unit = waspDB.insert[TopicModel](topicModel)
}