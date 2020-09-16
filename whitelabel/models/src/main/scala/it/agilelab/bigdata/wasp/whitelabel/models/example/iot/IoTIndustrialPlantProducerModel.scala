package it.agilelab.bigdata.wasp.whitelabel.models.example.iot

import it.agilelab.bigdata.wasp.models.ProducerModel

private[wasp] object IoTIndustrialPlantProducerModel {

  val producerName: String = "IoTIndustrialPlantProducer"

  lazy val iotIndustrialPlantProducer = ProducerModel (
    name = producerName,
    className = "it.agilelab.bigdata.wasp.whitelabel.producers.iot.IoTIndustrialPlantProducerGuardian",
    topicName = Some("industrial-plant.topic"),
    isActive = false,
    configuration = None,
    isRemote = false,
    isSystem = false
  )

}
