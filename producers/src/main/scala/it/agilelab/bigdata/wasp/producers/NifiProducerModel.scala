package it.agilelab.bigdata.wasp.producers

import it.agilelab.bigdata.wasp.core.models.ProducerModel
import org.mongodb.scala.bson.{BsonDocument, BsonObjectId, BsonString}

import scala.util.parsing.json.JSON

object NifiProducerModel {

  lazy val nifiProducer = ProducerModel(
    name = "NifiProducerGuardian",
    className = "it.agilelab.bigdata.wasp.producers.NifiProducerGuardian",
    id_topic = None,
    isActive = false,
    configuration = Some("c257f39a-015e-1000-c58b-ec0b29141625"),
    isRemote = false,
    isSystem = false,
    _id = Some(BsonObjectId())
  )
}
