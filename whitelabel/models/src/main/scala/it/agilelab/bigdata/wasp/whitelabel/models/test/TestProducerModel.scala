package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.core.models.ProducerModel

private[wasp] object TestProducerModel {

  object JSON {
    def apply() = ProducerModel(
      name = "TestJSONProducer",
      className = "it.agilelab.bigdata.wasp.whitelabel.producers.TestProducerGuardian",
      topicName = Some(TestTopicModel.testJsonTopic.name),
      isActive = false,
      configuration = None,
      isRemote = false,
      isSystem = false
    )
  }

  object AVRO {
    def apply() = ProducerModel(
      name = "TestAVROProducer",
      className = "it.agilelab.bigdata.wasp.whitelabel.producers.TestProducerGuardian",
      topicName = Some(TestTopicModel.testAvroTopic.name),
      isActive = false,
      configuration = None,
      isRemote = false,
      isSystem = false
    )
  }
}
