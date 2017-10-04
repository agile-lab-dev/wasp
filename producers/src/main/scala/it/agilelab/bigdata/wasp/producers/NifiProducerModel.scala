package it.agilelab.bigdata.wasp.producers

import java.io.File

import it.agilelab.bigdata.wasp.core.models.ProducerModel
import org.apache.commons.lang3.SerializationUtils
import org.mongodb.scala.bson.BsonObjectId
import spray.json._

import NifiRquestJsonProtocol._

object NifiProducerModel {

  // TODO Creazione configuration attraverso mapping con case class
  val nifiRequest = "{\"action\":\"\",\n\"child\":\n[\n    {\n    \"id\": \"64cdcd10-6ccc-3452-44e8-5f2ccbdfe19b\",\n    \"edge\": [\"cd17f96e-2b72-32c8-06d2-b5139cae00ac\"],\n \"data\": \"\"    }\n]\n}"

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