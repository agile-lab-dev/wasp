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
    configuration = Some("{\"action\":\"\",\"id\":\"fcd3d539-f175-30a5-542a-d195d0ca1bd6\",\"id_rpg\":[{\"id_platform\":\"02dcc135-e883-3f42-6674-f5568fa2e33f\",\"id_edge\":[\"9cfd6760-2d1e-3c01-2bf5-ddacae69251b\",\"9cfd6760-2d1e-3c01-2bf5-ddacae69251b\"]}]}"),
    isRemote = false,
    isSystem = false,
    _id = Some(BsonObjectId())
  )
}
