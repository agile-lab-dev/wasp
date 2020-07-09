package it.agilelab.bigdata.wasp.whitelabel.producers.eventengine

import java.io.ByteArrayOutputStream

import akka.actor.{ActorRef, Props}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.sksamuel.avro4s.{AvroJsonOutputStream, SchemaFor, ToRecord}
import it.agilelab.bigdata.wasp.repository.core.bl.{ProducerBL, TopicBL}
import it.agilelab.bigdata.wasp.models.TopicModel
import it.agilelab.bigdata.wasp.producers.{ProducerActor, ProducerGuardian, StartMainTask}
import it.agilelab.bigdata.wasp.whitelabel.models.example.FakeData
import spray.json.DefaultJsonProtocol

import scala.concurrent.ExecutionContext
import scala.util.Random

class FakeDataProducerGuardian (env: {val producerBL: ProducerBL; val topicBL: TopicBL}, producerName: String)
  extends ProducerGuardian(env.asInstanceOf[AnyRef {val producerBL: ProducerBL; val topicBL: TopicBL}], producerName) {
  override val name: String = ""

  override def startChildActors(): Unit = {

    val aRef = context.actorOf(Props(new FakeDataProducerActor(kafka_router, associatedTopic)))
    logger.info("Created actor")
    aRef ! StartMainTask
  }
}

private[producers] class FakeDataProducerActor(kafka_router: ActorRef, topic: Option[TopicModel])
  extends ProducerActor[FakeData](kafka_router, topic)
    with SprayJsonSupport with DefaultJsonProtocol {

  val rand = new Random()

  override def retrievePartitionKey: FakeData => String =
    (data: FakeData) => data.hashCode.toString





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

  private def generateRandomData(): FakeData =
    FakeData(s"sensor_${rand.nextInt(101)}", rand.nextFloat() * 200, System.currentTimeMillis(), if (rand.nextInt(2) % 2 == 0) "even" else "odd", rand.nextInt(101))



  def generateOutputJsonMessage(input: FakeData): String = {

    val baos = new ByteArrayOutputStream()
    val output =
      AvroJsonOutputStream[FakeData](baos)(SchemaFor[FakeData], ToRecord[FakeData])
    output.write(input)
    output.close()
    baos.toString("UTF-8")
  }

  def generateRawOutputJsonMessage(input: FakeData): String = {

    val baos = new ByteArrayOutputStream()
    val output =
      AvroJsonOutputStream[FakeData](baos)(SchemaFor[FakeData], ToRecord[FakeData])

    output.write(input)
    output.close()
    baos.toString("UTF-8")
  }
}