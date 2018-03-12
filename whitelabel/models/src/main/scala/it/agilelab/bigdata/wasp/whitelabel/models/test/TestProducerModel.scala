package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.core.models.ProducerModel

private[wasp] object TestProducerModel {

  lazy val json = ProducerModel(
      name = "TestJSONProducer",
      className = "it.agilelab.bigdata.wasp.whitelabel.producers.test.TestProducerGuardian",
      topicName = Some(TestTopicModel.json.name),
      isActive = false,
      configuration = None,
      isRemote = false,
      isSystem = false
    )

  lazy val avro  = ProducerModel(
      name = "TestAVROProducer",
      className = "it.agilelab.bigdata.wasp.whitelabel.producers.test.TestProducerGuardian",
      topicName = Some(TestTopicModel.avro.name),
      isActive = false,
      configuration = None,
      isRemote = false,
      isSystem = false
    )

}
