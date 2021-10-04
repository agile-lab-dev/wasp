package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.{ProducerBL, TopicBL}
import it.agilelab.bigdata.wasp.models.{ProducerModel, TopicModel}
import it.agilelab.bigdata.wasp.repository.core.dbModels.ProducerDBModel
import it.agilelab.bigdata.wasp.repository.core.mappers.ProducerDBModelMapperSelector.applyMap
import it.agilelab.bigdata.wasp.repository.core.mappers.ProducerMapperV1
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.bson.BsonBoolean
import org.mongodb.scala.bson.BsonString


class ProducerBLImp(waspDB: WaspMongoDB) extends ProducerBL {


  def getByName(name: String): Option[ProducerModel] = {

    waspDB.getDocumentByField[ProducerDBModel]("name", new BsonString(name))
      .map(applyMap)
  }


  def getActiveProducers(isActive: Boolean = true): Seq[ProducerModel] = {
    waspDB.getAllDocumentsByField[ProducerDBModel]("isActive", new BsonBoolean(isActive))
      .map(applyMap)
  }

  def getSystemProducers: Seq[ProducerModel] = {
    waspDB.getAllDocumentsByField[ProducerDBModel]("isSystem", new BsonBoolean(true))
      .map(applyMap)
  }

  def getNonSystemProducers: Seq[ProducerModel] = {
    waspDB.getAllDocumentsByField[ProducerDBModel]("isSystem", new BsonBoolean(false))
      .map(applyMap)
  }

  def getByTopicName(topicName: String): Seq[ProducerModel] = {
    waspDB.getAllDocumentsByField[ProducerDBModel]("topicName", BsonString(topicName))
      .map(applyMap)
  }

  def getTopic(topicBL: TopicBL, producerModel: ProducerModel): Option[TopicModel] = {
    if (producerModel.hasOutput)
      topicBL.getTopicModelByName(producerModel.topicName.get)
    else
      None
  }

  def getAll: Seq[ProducerModel] = {
    waspDB.getAll[ProducerDBModel]()
      .map(applyMap)

  }

  // use newest mapper
  def update(producerModel: ProducerModel): Unit = {
    waspDB.updateByName[ProducerDBModel](producerModel.name,
      ProducerMapperV1.fromModelToDBModel(producerModel))
  }

  override def persist(producerModel: ProducerModel): Unit =
    waspDB.insert[ProducerDBModel](ProducerMapperV1.fromModelToDBModel(producerModel))

  override def insertIfNotExists(producerModel: ProducerModel): Unit =
    waspDB.insertIfNotExists[ProducerDBModel](ProducerMapperV1.fromModelToDBModel(producerModel))

  override def upsert(producerModel: ProducerModel): Unit =
    waspDB.upsert[ProducerDBModel](ProducerMapperV1.fromModelToDBModel(producerModel))
}
