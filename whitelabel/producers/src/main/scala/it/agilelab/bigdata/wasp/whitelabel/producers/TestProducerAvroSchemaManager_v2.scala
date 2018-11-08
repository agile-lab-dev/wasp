package it.agilelab.bigdata.wasp.whitelabel.producers

import java.io.ByteArrayOutputStream
import java.util.UUID

import akka.actor.{ActorRef, Props}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.stream.ActorMaterializer
import com.sksamuel.avro4s.AvroJsonOutputStream
import it.agilelab.bigdata.wasp.core.bl.{ProducerBL, TopicBL}
import it.agilelab.bigdata.wasp.core.models.TopicModel
import it.agilelab.bigdata.wasp.producers.{ProducerActor, ProducerGuardian, StartMainTask}
import it.agilelab.bigdata.wasp.whitelabel.models.test.TopicAvro_v2
import spray.json.DefaultJsonProtocol

/**
  * @author andreaL
  */

final class TestProducerAvroSchemaManager_v2(env: {val producerBL: ProducerBL; val topicBL: TopicBL}, producerId: String)
  extends ProducerGuardian(env.asInstanceOf[AnyRef {val producerBL: ProducerBL; val topicBL: TopicBL}], producerId) with SprayJsonSupport with DefaultJsonProtocol {

  implicit val materializer = ActorMaterializer()
  implicit val system = this.context.system
  override val name: String = "CrashOctoProducer"

  def startChildActors() = {
    logger.info(s"Starting get data on ${cluster.selfAddress}")

    val aRef = context.actorOf(Props(new TestActorAvroSchemaManager_v2("", kafka_router, associatedTopic)))
    println("created actor")
    aRef ! StartMainTask
    println("told actor to start main task")

  }
}

/** For simplicity, these just go through Akka. */

private[wasp] class TestActorAvroSchemaManager_v2(filePath: String, kafka_router: ActorRef, topic: Option[TopicModel])
  extends ProducerActor[TopicAvro_v2](kafka_router, topic)
    with SprayJsonSupport with DefaultJsonProtocol {


  override def preStart(): Unit = {
    logger.info(s"Starting producing TestActorAvroSchemaManager")
    println("in pre start")
  }

  def mainTask(): Unit = {
    (0 to 10).foreach( x => {
      println(s"TopicAvro_v2: ${x}")
      Thread.sleep(2000)
      sendMessage(TopicAvro_v2(UUID.randomUUID().toString, Math.random().toInt, Math.random().toInt, Some((Math.random()*10).toString)))
    })
  }

  /**
    * Used when writing to topics with data type "json" or "avro"
    */
  override def generateOutputJsonMessage(input: TopicAvro_v2): String = {
    val baos = new ByteArrayOutputStream()
    val output = AvroJsonOutputStream[TopicAvro_v2](baos)
    output.write(input)
    output.close()
    baos.toString("UTF-8")
  }

  /**
    * Defines a function to extract the key to be used to identify the landing partition in kafka topic,
    * given a message of type T
    *
    * @return a function that extract the partition key as String from the T instance to be sent to kafka
    */
  override def retrievePartitionKey: TopicAvro_v2 => String = t => t.id
}