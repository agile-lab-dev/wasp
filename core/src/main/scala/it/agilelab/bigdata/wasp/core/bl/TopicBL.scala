package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.TopicModel
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.bson.BsonValue
import org.mongodb.scala.bson.{BsonDocument, BsonObjectId, BsonString}

trait TopicBL {

  def getByName(name: String): Option[TopicModel]

  def getAll : Seq[TopicModel]

  def persist(topicModel: TopicModel): Unit

}

class TopicBLImp(waspDB: WaspDB) extends TopicBL  {

  private def factory(t: BsonDocument): TopicModel =
    new TopicModel(t.get("name").asString().getValue,
                   t.get("creationTime").asInt64().getValue,
                   t.get("partitions").asInt32().getValue,
                   t.get("replicas").asInt32().getValue,
                   t.get("topicDataType").asString().getValue,
                   if (t.containsKey("keyFieldName"))
                     Some(t.get("keyFieldName").asString().getValue)
                   else
                     None,
                   if (t.containsKey("headersFieldName"))
                     Some(t.get("headersFieldName").asString().getValue)
                   else
                     None,
                   t.get("schema").asDocument())

  def getByName(name: String): Option[TopicModel] = {
    waspDB.getDocumentByFieldRaw[TopicModel]("name", new BsonString(name)).map(topic => {
      factory(topic)
    })
  }


  def getAll: Seq[TopicModel] = {
    waspDB.getAllRaw[TopicModel]().map(factory)
  }

  override def persist(topicModel: TopicModel): Unit = waspDB.insert[TopicModel](topicModel)
}