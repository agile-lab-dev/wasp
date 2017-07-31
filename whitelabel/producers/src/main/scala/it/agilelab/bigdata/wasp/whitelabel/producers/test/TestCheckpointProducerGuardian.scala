package it.agilelab.bigdata.wasp.whitelabel.producers.test

import akka.actor.{ActorRef, Props}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import it.agilelab.bigdata.wasp.core.bl.{ProducerBL, TopicBL}
import it.agilelab.bigdata.wasp.core.models.TopicModel
import it.agilelab.bigdata.wasp.producers.{ProducerActor, ProducerGuardian, StartMainTask}
import it.agilelab.bigdata.wasp.whitelabel.models.test.TestCheckpointDocument
import spray.json.DefaultJsonProtocol

import scala.concurrent.ExecutionContext

final class TestCheckpointProducerGuardian(env: {val producerBL: ProducerBL; val topicBL: TopicBL}, producerName: String)
  extends ProducerGuardian(env, producerName) {

  override val name: String = "testCheckpointProducerGuardian"

  override def startChildActors(): Unit = {

    val aRef = context.actorOf(Props(new TestCheckpointActor(kafka_router, associatedTopic)))
    logger.info("Created actor")
    aRef ! StartMainTask
  }
}

private[producers] class TestCheckpointActor(kafka_router: ActorRef, topic: Option[TopicModel])
  extends ProducerActor[TestCheckpointDocument](kafka_router, topic)
    with SprayJsonSupport with DefaultJsonProtocol {


  override def retrievePartitionKey: TestCheckpointDocument => String = (td: TestCheckpointDocument) => td.id

  def createTestCheckpointDocument(documentId: Int) = {

    TestCheckpointDocument("v1", ""+documentId, 1)
  }

  var documentId = 0

  override def mainTask() = {
    logger.info(s"Starting main task for actor: ${this.getClass.getName}")

    val doc = createTestCheckpointDocument(documentId)
    logger.info(s"TestCheckpointDocument ${documentId} CREATED!")
    sendMessage(doc)

    documentId += 1
    import scala.concurrent.duration._
    implicit val executor: ExecutionContext = context.dispatcher
    system.scheduler.scheduleOnce(1 seconds) {
      self ! StartMainTask
    }
  }

  override def generateOutputJsonMessage(input: TestCheckpointDocument): String = {
    val testCheckpointDocumentToJson = jsonFormat6(TestCheckpointDocument.apply)
    val jsonObj = testCheckpointDocumentToJson.write(input)
    jsonObj.compactPrint
  }
}