package it.agilelab.bigdata.wasp.producers

import it.agilelab.bigdata.wasp.core.models.ProducerModel
import org.mongodb.scala.bson.{BsonDocument, BsonObjectId, BsonString}

object NifiProducerModel {

  lazy val nifiProducer = ProducerModel(
    name = "NifiProducer",
    className = "it.agilelab.bigdata.wasp.master.NifiProducer",
    id_topic = None,
    isActive = false,
    configuration = Some(BsonDocument("id" -> "value")), // TODO: Set correct ProcessGroup ID
    isRemote = false,
    isSystem = false,
    _id = Some(BsonObjectId())
  )
}
