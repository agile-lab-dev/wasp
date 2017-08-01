package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.{BSONConversionHelper, TopicModel}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import reactivemongo.bson.{BSONObjectID, BSONString}
import reactivemongo.api.commands.WriteResult

import scala.concurrent.Future

trait TopicBL {

  def getByName(name: String): Future[Option[TopicModel]]

  def getById(id: String): Future[Option[TopicModel]]

  def getAll : Future[List[TopicModel]]

  def persist(topicModel: TopicModel): Future[WriteResult]

}

class TopicBLImp(waspDB: WaspDB) extends TopicBL with BSONConversionHelper {

  import scala.concurrent.ExecutionContext.Implicits.global

  private def factory(t: TopicModel) = new TopicModel(t.name, t.creationTime, t.partitions, t.replicas, t.topicDataType, t.schema, t._id)

  def getByName(name: String): Future[Option[TopicModel]] = {
    waspDB.getDocumentByField[TopicModel]("name", new BSONString(name)).map(topic => {
      topic.map(p => factory(p))
    })
  }

  def getById(id: String): Future[Option[TopicModel]] = {
    waspDB.getDocumentByID[TopicModel](BSONObjectID(id)).map(Topic => {
      Topic.map(p => factory(p))
    })
  }

  def getAll = {
    waspDB.getAll[TopicModel]
  }

  override def persist(topicModel: TopicModel): Future[WriteResult] = waspDB.insert[TopicModel](topicModel)
}