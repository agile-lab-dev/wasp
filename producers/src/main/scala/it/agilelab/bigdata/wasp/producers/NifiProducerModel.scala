package it.agilelab.bigdata.wasp.producers

import it.agilelab.bigdata.wasp.models.ProducerModel
import org.mongodb.scala.bson.BsonObjectId

object NifiProducerModel {

  val nifiRequest: String =
    """{
      |"request":
      |{
      |  "scheme":"http",
      |  "host":"172.22.0.2",
      |  "port":1080
      |},
      |"child":[
      |  {
      |    "id":"64cdcd10-6ccc-3452-f4b0-452a3069b04b",
      |    "edge":["70c05e86-2ef1-3f6d-b6d5-2f6f880b27c8"]
      |  }
      |]
      |}""".stripMargin

  lazy val nifiProducer = ProducerModel(
    name = "NifiProducerGuardian",
    className = "it.agilelab.bigdata.wasp.producers.NifiProducerGuardian",
    topicName = None,
    isActive = false,
    configuration = Some(nifiRequest),
    isRemote = false,
    isSystem = false)
}