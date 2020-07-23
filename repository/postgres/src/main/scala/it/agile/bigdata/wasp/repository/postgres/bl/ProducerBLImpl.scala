package it.agile.bigdata.wasp.repository.postgres.bl

import it.agile.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agile.bigdata.wasp.repository.postgres.tables.{ProducerTableDefinition, TableDefinition}
import it.agilelab.bigdata.wasp.models.{ProducerModel, TopicModel}
import it.agilelab.bigdata.wasp.repository.core.bl.{ProducerBL, TopicBL}

case class ProducerBLImpl(waspDB : WaspPostgresDB) extends ProducerBL with PostgresBL {

  implicit val tableDefinition: TableDefinition[ProducerModel,String] = ProducerTableDefinition

  override def getByName(name: String): Option[ProducerModel] = waspDB.getByPrimaryKey(name)

  override def getActiveProducers(isActive: Boolean): Seq[ProducerModel] =
    waspDB.getBy(Array((ProducerTableDefinition.isActive,isActive)))

  override def getSystemProducers: Seq[ProducerModel] =
    waspDB.getBy(Array((ProducerTableDefinition.isSystem,true)))

  override def getNonSystemProducers: Seq[ProducerModel] =
    waspDB.getBy(Array((ProducerTableDefinition.isSystem,false)))

  override def getByTopicName(name: String): Seq[ProducerModel] =
    waspDB.getBy(Array((ProducerTableDefinition.topicName,name)))

  override def getTopic(topicBL: TopicBL, producerModel: ProducerModel): Option[TopicModel] = {
    if (producerModel.hasOutput)
      topicBL.getTopicModelByName(producerModel.topicName.get)
    else
      None
  }

  override def getAll: Seq[ProducerModel] = waspDB.getAll()

  override def update(producerModel: ProducerModel): Unit = waspDB.updateByPrimaryKey(producerModel)

  override def persist(producerModel: ProducerModel): Unit = waspDB.insert(producerModel)

  override def upsert(producerModel: ProducerModel): Unit = waspDB.upsert(producerModel)

  override def insertIfNotExists(producerModel: ProducerModel): Unit = waspDB.insertIfNotExists(producerModel)

  override def createTable(): Unit = waspDB.createTable()
}
