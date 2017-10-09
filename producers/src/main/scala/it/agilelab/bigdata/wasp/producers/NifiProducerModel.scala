package it.agilelab.bigdata.wasp.producers

import it.agilelab.bigdata.wasp.core.models.ProducerModel
import org.mongodb.scala.bson.BsonObjectId

object NifiProducerModel {

  val nifiRequest: String =
    """{
      |"request":
      |{
      |  "scheme":"http",
      |  "host":"localhost",
      |  "port":1080
      |},
      |"child":[
      |  {
      |    "id":"64cdcd10-6ccc-3452-44e8-5f2ccbdfe19b",
      |    "edge":["cd17f96e-2b72-32c8-06d2-b5139cae00ac"]
      |  }
      |]
      |}""".stripMargin

  lazy val nifiProducer = ProducerModel(
    name = "NifiProducerGuardian",
    className = "it.agilelab.bigdata.wasp.producers.NifiProducerGuardian",
    id_topic = None,
    isActive = false,
    configuration = Some(nifiRequest),
    isRemote = false,
    isSystem = false,
    _id = Some(BsonObjectId())
  )
}