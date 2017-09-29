package it.agilelab.bigdata.wasp.producers

import java.io.File

import it.agilelab.bigdata.wasp.core.models.ProducerModel
import org.apache.commons.lang3.SerializationUtils
import org.mongodb.scala.bson.{BsonObjectId}
import spray.json._
import NifiRquestJsonProtocol._

object NifiProducerModel {

  val edge1 = List(NifiPlatform("4db1f23f-ac33-303b-6720-4d6391c79b", Some(List("afa3f732-7607-3bc7-4386-e6aebdabd2a1"))))

  val routingInfo = NifiRequest("", None, Some(edge1))

  val mlModel = SerializationUtils.serialize(new File("/home/amarino/RegressionSumModels_1"))

  val configuration = Configuration(routingInfo, Some(mlModel)).toJson.toString

  lazy val nifiProducer = ProducerModel(
    name = "NifiProducerGuardian",
    className = "it.agilelab.bigdata.wasp.producers.NifiProducerGuardian",
    id_topic = None,
    isActive = false,
    configuration = Some(configuration),
    isRemote = false,
    isSystem = false,
    _id = Some(BsonObjectId())
  )
}
