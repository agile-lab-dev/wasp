package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.{ProducerModel, TopicModel}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.bson.BsonBoolean
import org.mongodb.scala.bson.{BsonObjectId, BsonString}

trait ProducerBL {

  def getByName(name: String): Option[ProducerModel]

  def getActiveProducers(isActive: Boolean = true): Seq[ProducerModel]
  
  def getSystemProducers: Seq[ProducerModel]
  
  def getNonSystemProducers: Seq[ProducerModel]
  
  def getByTopicName(name: String): Seq[ProducerModel]

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

  private def factory(p: ProducerModel) = ProducerModel(p.name, p.className, p.topicName, p.isActive, p.configuration, p.isRemote, p.isSystem)

  def getByName(name: String): Option[ProducerModel] = {
    waspDB.getDocumentByField[ProducerModel]("name", new BsonString(name)).map(factory)
  }


  def getActiveProducers(isActive: Boolean = true): Seq[ProducerModel] = {
    waspDB.getAllDocumentsByField[ProducerModel]("isActive", new BsonBoolean(isActive)).map(factory)
  }
  
  def getSystemProducers: Seq[ProducerModel] = {
    waspDB.getAllDocumentsByField[ProducerModel]("isSystem", new BsonBoolean(true)).map(factory)
  }
  
  def getNonSystemProducers: Seq[ProducerModel] = {
    waspDB.getAllDocumentsByField[ProducerModel]("isSystem", new BsonBoolean(false)).map(factory)
  }

  def getByTopicName(topicName: String): Seq[ProducerModel] = {
    waspDB.getAllDocumentsByField[ProducerModel]("topicName", BsonString(topicName)).map(factory)
  }

  def getTopic(topicBL: TopicBL, producerModel: ProducerModel): Option[TopicModel] = {
    if (producerModel.hasOutput)
      topicBL.getByName(producerModel.topicName.get)
    else
        None
  }

  def getAll: Seq[ProducerModel] = {
    waspDB.getAll[ProducerModel]().map(factory)
  }

  def update(producerModel: ProducerModel): Unit = {
    waspDB.updateByName[ProducerModel](producerModel.name, producerModel)
  }

  override def persist(producerModel: ProducerModel): Unit = waspDB.insert[ProducerModel](producerModel)
}