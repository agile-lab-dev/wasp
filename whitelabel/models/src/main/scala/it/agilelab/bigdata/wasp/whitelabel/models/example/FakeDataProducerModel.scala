package it.agilelab.bigdata.wasp.whitelabel.models.example

import it.agilelab.bigdata.wasp.models.ProducerModel

private[wasp] object FakeDataProducerModel {

  val producerName: String = "FakeDataProducer"

  lazy val fakeDataProducerSimulator = ProducerModel (
    name = producerName,
    className = "it.agilelab.bigdata.wasp.whitelabel.producers.eventengine.FakeDataProducerGuardian",
    topicName = Some("fake-data.topic"),
    isActive = false,
    configuration = None,
    isRemote = false,
    isSystem = false
  )

}
