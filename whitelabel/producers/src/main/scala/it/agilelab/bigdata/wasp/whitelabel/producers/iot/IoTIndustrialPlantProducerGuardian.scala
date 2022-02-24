package it.agilelab.bigdata.wasp.whitelabel.producers.iot

import java.io.ByteArrayOutputStream

import akka.actor.{ActorRef, Props}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.sksamuel.avro4s.{AvroJsonOutputStream, SchemaFor, ToRecord}
import it.agilelab.bigdata.wasp.models.TopicModel
import it.agilelab.bigdata.wasp.producers.{ProducerActor, ProducerGuardian, StartMainTask}
import it.agilelab.bigdata.wasp.repository.core.bl.{ProducerBL, TopicBL}
import it.agilelab.bigdata.wasp.whitelabel.models.example.iot.{FakeIndustrialPlantData, IndustrialPlantData}
import spray.json.DefaultJsonProtocol

import scala.concurrent.ExecutionContext
import scala.util.Random

class IoTIndustrialPlantProducerGuardian (env: {val producerBL: ProducerBL; val topicBL: TopicBL}, producerName: String)
  extends ProducerGuardian(env.asInstanceOf[AnyRef {val producerBL: ProducerBL; val topicBL: TopicBL}], producerName) {
  override val name: String = ""

  override def startChildActors(): Unit = {

    val aRef = context.actorOf(Props(new IoTIndustrialPlantProducerActor(kafka_router, associatedTopic)))
    logger.info("Created actor")
    aRef ! StartMainTask
  }
}

private[producers] class IoTIndustrialPlantProducerActor(kafka_router: ActorRef, topic: Option[TopicModel])
  extends ProducerActor[IndustrialPlantData](kafka_router, topic)
    with SprayJsonSupport with DefaultJsonProtocol {

  val rand = new Random()

  override def retrievePartitionKey: IndustrialPlantData => String =
    (data: IndustrialPlantData) => data.plant





  def sendMsg(): Unit = {
    val data = generateRandomData()
    sendMessage(data)
  }

  override def mainTask(): Unit = {
    //logger.info(s"Starting main task for actor: ${this.getClass.getName}")
    import scala.concurrent.duration._
    implicit val executor: ExecutionContext = context.dispatcher

    val data = generateRandomData()
    sendMessage(data)

    system.scheduler.scheduleOnce(10 millis, self, StartMainTask)

  }

  private def generateRandomData(): IndustrialPlantData = FakeIndustrialPlantData.fromRandom()



  def generateOutputJsonMessage(input: IndustrialPlantData): String = {

    val baos = new ByteArrayOutputStream()
    val output =
      AvroJsonOutputStream[IndustrialPlantData](baos)(SchemaFor[IndustrialPlantData], ToRecord[IndustrialPlantData])
    output.write(input)
    output.close()
    baos.toString("UTF-8")
  }

  def generateRawOutputJsonMessage(input: IndustrialPlantData): String = {

    val baos = new ByteArrayOutputStream()
    val output =
      AvroJsonOutputStream[IndustrialPlantData](baos)(SchemaFor[IndustrialPlantData], ToRecord[IndustrialPlantData])

    output.write(input)
    output.close()
    baos.toString("UTF-8")
  }
}