package it.agilelab.bigdata.wasp.producers

import java.io.File

import it.agilelab.bigdata.wasp.core.models.ProducerModel
import org.apache.commons.lang3.SerializationUtils
import org.mongodb.scala.bson.BsonObjectId
import spray.json._

import NifiRquestJsonProtocol._

object NifiProducerModel {

  /*val action = ""
  val id = None
  val all_child = Some(child1)
    val child1 = List(NifiPlatform("4db1f23f-ac33-303b-6720-4d6391c79b", edge))
      val edge = Some(List("afa3f732-7607-3bc7-4386-e6aebdabd2a1"))*/
  //val data = SerializationUtils.serialize(new File("/home/amarino/RegressionSumModels_1")).toJson.toString

  //val nifiRequest = NifiRequest(action, id, all_child, data).toJson.toString
  val nifiRequest = "{\"action\":\"RUNNING\",\n\"child\":\n[\n    {\"id\": \"c9c86be7-c7e5-35f2-1ef1-eb3270dde40d\",\n    \"edge\": [\"8062d204-c379-31e4-25a8-042b1de389b6\"]\n    }\n]\n}"

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