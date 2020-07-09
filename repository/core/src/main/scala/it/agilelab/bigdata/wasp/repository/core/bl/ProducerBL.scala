package it.agilelab.bigdata.wasp.repository.core.bl

import it.agilelab.bigdata.wasp.models.{ProducerModel, TopicModel}

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

  def upsert(producerModel: ProducerModel): Unit

  def insertIfNotExists(producerModel: ProducerModel): Unit
}