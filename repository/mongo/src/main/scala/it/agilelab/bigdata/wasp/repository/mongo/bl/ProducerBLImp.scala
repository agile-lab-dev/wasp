package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.{ProducerBL, TopicBL}
import it.agilelab.bigdata.wasp.models.{ProducerModel, TopicModel}
import it.agilelab.bigdata.wasp.repository.core.dbModels.{ProducerDBModel, ProducerDBModelV1}
import it.agilelab.bigdata.wasp.repository.core.mappers.ProducerDBModelMapperSelector.factory
import it.agilelab.bigdata.wasp.repository.core.mappers.ProducerMapperV1.transform
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.bson.BsonBoolean
import org.mongodb.scala.bson.BsonString


class ProducerBLImp(waspDB: WaspMongoDB) extends ProducerBL {


  def getByName(name: String): Option[ProducerModel] = {

    waspDB.getDocumentByField[ProducerDBModel]("name", new BsonString(name))
      .map(factory)
  }


  def getActiveProducers(isActive: Boolean = true): Seq[ProducerModel] = {
    waspDB.getAllDocumentsByField[ProducerDBModel]("isActive", new BsonBoolean(isActive))
      .map(factory)
  }

  def getSystemProducers: Seq[ProducerModel] = {
    waspDB.getAllDocumentsByField[ProducerDBModel]("isSystem", new BsonBoolean(true))
      .map(factory)
  }

  def getNonSystemProducers: Seq[ProducerModel] = {
    waspDB.getAllDocumentsByField[ProducerDBModel]("isSystem", new BsonBoolean(false))
      .map(factory)
  }

  def getByTopicName(topicName: String): Seq[ProducerModel] = {
    waspDB.getAllDocumentsByField[ProducerDBModel]("topicName", BsonString(topicName))
      .map(factory)
  }

  def getTopic(topicBL: TopicBL, producerModel: ProducerModel): Option[TopicModel] = {
    if (producerModel.hasOutput)
      topicBL.getTopicModelByName(producerModel.topicName.get)
    else
      None
  }

  def getAll: Seq[ProducerModel] = {
    waspDB.getAll[ProducerDBModel]()
      .map(factory)

  }

  // use newest mapper
  def update(producerModel: ProducerModel): Unit = {
    waspDB.updateByName[ProducerDBModel](producerModel.name,
      transform[ProducerDBModelV1](producerModel))
  }

  override def persist(producerModel: ProducerModel): Unit =
    waspDB.insert[ProducerDBModel](transform[ProducerDBModelV1](producerModel))

  override def insertIfNotExists(producerModel: ProducerModel): Unit =
    waspDB.insertIfNotExists[ProducerDBModel](transform[ProducerDBModelV1](producerModel))

  override def upsert(producerModel: ProducerModel): Unit =
    waspDB.upsert[ProducerDBModel](transform[ProducerDBModelV1](producerModel))
}
