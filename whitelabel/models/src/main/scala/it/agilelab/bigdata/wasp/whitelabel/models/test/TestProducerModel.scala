package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.core.models.ProducerModel

object TestProducerModel {

  def apply(): ProducerModel = ProducerModel(
    name = "TestProducer",
    className = "it.agilelab.bigdata.wasp.whitelabel.producers.TestProducerGuardian",
    topicName = Some(TestTopicModel.testTopic.name),
    isActive = false,
    configuration = None,
    isRemote = false,
    isSystem = false
  )
}
