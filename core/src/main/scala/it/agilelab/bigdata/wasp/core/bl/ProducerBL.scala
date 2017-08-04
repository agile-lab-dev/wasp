package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.{ProducerModel, TopicModel}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.bson.BsonBoolean
import org.mongodb.scala.bson.{BsonObjectId, BsonString}

trait ProducerBL {

  def getByName(name: String): Option[ProducerModel]

  def getById(id: String): Option[ProducerModel]

  def getActiveProducers(isActive: Boolean = true): Seq[ProducerModel]

  def getByTopicId(id_topic: BsonObjectId): Seq[ProducerModel]

  def getTopic(topicBL: TopicBL, producerModel: ProducerModel): Option[TopicModel]

  def getAll: Seq[ProducerModel]

  def update(producerModel: ProducerModel): Unit

  def setIsActive(producerModel: ProducerModel, isActive: Boolean): Unit = {
    producerModel.isActive = isActive
    update(producerModel)
  }

  def persist(producerModel: ProducerModel): Unit
}

class ProducerBLImp(waspDB: WaspDB) extends  ProducerBL {

  private def factory(p: ProducerModel) = ProducerModel(p.name, p.className, p.id_topic, p.isActive, p.configuration, p.isRemote, p._id)

  def getByName(name: String): Option[ProducerModel] = {
    waspDB.getDocumentByField[ProducerModel]("name", new BsonString(name)).map(factory)
  }

  def getById(id: String): Option[ProducerModel] = {
    waspDB.getDocumentByID[ProducerModel](BsonObjectId(id)).map(factory)
  }

  def getActiveProducers(isActive: Boolean = true): Seq[ProducerModel] = {
    waspDB.getAllDocumentsByField[ProducerModel]("isActive", new BsonBoolean(isActive)).map(factory)
  }

  def getByTopicId(id_topic: BsonObjectId): Seq[ProducerModel] = {
    waspDB.getAllDocumentsByField[ProducerModel]("id_topic", id_topic).map(factory)
  }

  def getTopic(topicBL: TopicBL, producerModel: ProducerModel): Option[TopicModel] = {
    if (producerModel.hasOutput)
      topicBL.getById(producerModel.id_topic.get.asString().getValue)
    else
        None
  }

  def getAll: Seq[ProducerModel] = {
    waspDB.getAll[ProducerModel]().map(factory)
  }

  def update(producerModel: ProducerModel): Unit = {
    waspDB.updateById[ProducerModel](producerModel._id.get, producerModel)
  }

  override def persist(producerModel: ProducerModel): Unit = waspDB.insert[ProducerModel](producerModel)
}