package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.{ProducerBL, TopicBL}
import it.agilelab.bigdata.wasp.models.{ProducerModel, TopicModel}
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.bson.BsonBoolean
import org.mongodb.scala.bson.BsonString

class ProducerBLImp(waspDB: WaspMongoDB) extends  ProducerBL {

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
      topicBL.getTopicModelByName(producerModel.topicName.get)
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

  override def insertIfNotExists(producerModel: ProducerModel): Unit = waspDB.insertIfNotExists[ProducerModel](producerModel)

  override def upsert(producerModel: ProducerModel): Unit = waspDB.upsert[ProducerModel](producerModel)
}
