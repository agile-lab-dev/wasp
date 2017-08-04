package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.TopicModel
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.bson.BsonString
import org.mongodb.scala.bson.BsonObjectId

import scala.concurrent.Future

trait TopicBL {

  def getByName(name: String): Option[TopicModel]

  def getById(id: String): Option[TopicModel]

  def getAll : Seq[TopicModel]

  def persist(topicModel: TopicModel): Unit

}

class TopicBLImp(waspDB: WaspDB) extends TopicBL  {

  private def factory(t: TopicModel) = new TopicModel(t.name, t.creationTime, t.partitions, t.replicas, t.topicDataType, t.schema, t._id)

  def getByName(name: String): Option[TopicModel] = {
    waspDB.getDocumentByField[TopicModel]("name", new BsonString(name)).map(topic => {
      factory(topic)
    })
  }

  def getById(id: String): Option[TopicModel] = {
    waspDB.getDocumentByID[TopicModel](BsonObjectId(id)).map(topic => {
      factory(topic)
    })
  }

  def getAll: Seq[TopicModel] = {
    waspDB.getAll[TopicModel]()
  }

  override def persist(topicModel: TopicModel): Unit = waspDB.insert[TopicModel](topicModel)
}