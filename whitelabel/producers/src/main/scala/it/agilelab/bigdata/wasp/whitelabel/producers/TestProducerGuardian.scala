package it.agilelab.bigdata.wasp.whitelabel.producers

import akka.actor.{ActorRef, Props}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import it.agilelab.bigdata.wasp.core.bl.{ProducerBL, TopicBL}
import it.agilelab.bigdata.wasp.core.models.TopicModel
import it.agilelab.bigdata.wasp.producers.{ProducerActor, ProducerGuardian, StartMainTask}
import it.agilelab.bigdata.wasp.whitelabel.models.test.{TestDocument, TestNestedDocument}
import spray.json.DefaultJsonProtocol

import scala.concurrent.ExecutionContext

final class TestProducerGuardian (env: {val producerBL: ProducerBL; val topicBL: TopicBL}, producerId: String)
  extends ProducerGuardian(env, producerId) {
  override val name: String = "testProducerGuardian"

  override def startChildActors(): Unit = {
    logger.info(s"Starting get data on ${cluster.selfAddress}")


    val aRef = context.actorOf(Props(new TestActor(kafka_router, associatedTopic)))
    println("created actor")
    aRef ! StartMainTask
    println("told actor to start main task")
  }
}

private[producers] class TestActor(kafka_router: ActorRef, topic: Option[TopicModel])
  extends ProducerActor[TestDocument](kafka_router, topic)
    with SprayJsonSupport with DefaultJsonProtocol {

  def createTestDocument(documentId: Int) = {

    val nestedDocument = TestNestedDocument("field1_"+ documentId, documentId, Some("field3_"+ documentId))
    TestDocument(""+documentId, documentId, nestedDocument)
  }

  var documentId = 0

  def mainTask() = {
    println("in main task")
    logger.info(s"Starting main task for actor: ${this.getClass.getName}")

    val doc = createTestDocument(documentId)
    println("TestDocument CREATED!")

    sendMessage(doc)
    documentId += 1
    import scala.concurrent.duration._
    implicit val executor: ExecutionContext = context.dispatcher
    system.scheduler.scheduleOnce(1 seconds) {
      self ! StartMainTask
    }
  }

  override def generateOutputJsonMessage(input: TestDocument): String = {
    implicit val testNestedDocumentToJson = jsonFormat3(TestNestedDocument.apply)
    val testDocumentToJson = jsonFormat3(TestDocument.apply)
    val jsonObj = testDocumentToJson.write(input)
    jsonObj.compactPrint
  }
}