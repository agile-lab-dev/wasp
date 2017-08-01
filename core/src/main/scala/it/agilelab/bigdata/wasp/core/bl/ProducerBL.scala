package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.{BSONConversionHelper, ProducerModel, TopicModel}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import reactivemongo.bson.{BSONBoolean, BSONObjectID, BSONString}
import reactivemongo.api.commands.WriteResult

import scala.concurrent._

trait ProducerBL {

  def getByName(name: String): Future[Option[ProducerModel]]

  def getById(id: String): Future[Option[ProducerModel]]

  def getActiveProducers(isActive: Boolean = true): Future[List[ProducerModel]]

  def getByTopicId(id_topic: BSONObjectID): Future[List[ProducerModel]]

  def getTopic(topicBL: TopicBL, producerModel: ProducerModel): Future[Option[TopicModel]]

  def getAll: Future[List[ProducerModel]]

  def update(producerModel: ProducerModel): Future[WriteResult]

  def setIsActive(producerModel: ProducerModel, isActive: Boolean): Future[WriteResult] = {
    producerModel.isActive = isActive
    update(producerModel)
  }

  def persist(producerModel: ProducerModel): Future[WriteResult]
}

class ProducerBLImp(waspDB: WaspDB) extends  ProducerBL with BSONConversionHelper{
  import scala.concurrent.ExecutionContext.Implicits.global

  private def factory(p: ProducerModel) = ProducerModel(p.name, p.className, p.id_topic, p.isActive, p.configuration, p.isRemote, p._id)

  def getByName(name: String): Future[Option[ProducerModel]] = {
    waspDB.getDocumentByField[ProducerModel]("name", new BSONString(name)).map(producer => {
      producer.map(p => factory(p))
    })
  }

  def getById(id: String): Future[Option[ProducerModel]] = {
    waspDB.getDocumentByID[ProducerModel](BSONObjectID(id)).map(producer => {
      producer.map(p => factory(p))
    })
  }

  def getActiveProducers(isActive: Boolean = true): Future[List[ProducerModel]] = {
    waspDB.getAllDocumentsByField[ProducerModel]("isActive", new BSONBoolean(isActive)).map(producer => {
      producer.map(p => factory(p))
    })
  }

  def getByTopicId(id_topic: BSONObjectID): Future[List[ProducerModel]] = {
    waspDB.getAllDocumentsByField[ProducerModel]("id_topic", id_topic).map(producer => {
      producer.map(p => factory(p))
    })
  }

  def getTopic(topicBL: TopicBL, producerModel: ProducerModel): Future[Option[TopicModel]] = {
    if (producerModel.hasOutput)
      topicBL.getById(producerModel.id_topic.get.stringify)
    else
      future {
        None
      }
  }

  def getAll: Future[List[ProducerModel]] = {
    waspDB.getAll[ProducerModel].map(producer => {
      producer.map(p => factory(p))
    })
  }

  def update(producerModel: ProducerModel): Future[WriteResult] = {
    waspDB.updateById[ProducerModel](producerModel._id.get, producerModel)
  }

  override def persist(producerModel: ProducerModel): Future[WriteResult] = waspDB.insert[ProducerModel](producerModel)
}