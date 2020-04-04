package it.agilelab.bigdata.wasp.whitelabel.producers.test

import akka.actor.{ActorRef, Props}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import it.agilelab.bigdata.wasp.core.bl.{ProducerBL, TopicBL}
import it.agilelab.bigdata.wasp.core.models.TopicModel
import it.agilelab.bigdata.wasp.producers.{ProducerActor, ProducerGuardian, StartMainTask}
import it.agilelab.bigdata.wasp.whitelabel.models.test.{TestDocumentHbaseMultiClustering, TestNestedDocument}
import spray.json.DefaultJsonProtocol

import scala.concurrent.ExecutionContext

final class TestHbaseMultiClusteringProducerGuardian(env: {val producerBL: ProducerBL; val topicBL: TopicBL}, producerName: String)
  extends ProducerGuardian(env, producerName) {

  override val name: String = "testHbaseMultiClusteringProducerGuardian"

  override def startChildActors(): Unit = {

    val aRef = context.actorOf(Props(new TestHbaseMultiClusteringActor(kafka_router, associatedTopic)))
    logger.info("Created actor")
    aRef ! StartMainTask
  }
}

private[producers] class TestHbaseMultiClusteringActor(kafka_router: ActorRef, topic: Option[TopicModel])
  extends ProducerActor[TestDocumentHbaseMultiClustering](kafka_router, topic)
    with SprayJsonSupport with DefaultJsonProtocol {


  override def retrievePartitionKey: TestDocumentHbaseMultiClustering => String = (td: TestDocumentHbaseMultiClustering) => td.id

  def createTestDocument(documentId: Int) = {

    val nestedDocument = TestNestedDocument("field1_"+ documentId, documentId, Some("field3_"+ documentId))
    TestDocumentHbaseMultiClustering(""+documentId, documentId, nestedDocument, documentId)
  }

  var documentId = 0

  override def mainTask() = {
    logger.info(s"Starting main task for actor: ${this.getClass.getName}")

    val doc = createTestDocument(documentId)
    logger.info(s"TestDocument ${documentId} CREATED!")
    sendMessage(doc)

    documentId += 1
    import scala.concurrent.duration._
    implicit val executor: ExecutionContext = context.dispatcher
    system.scheduler.scheduleOnce(1 seconds) {
      self ! StartMainTask
    }
  }

  override def generateOutputJsonMessage(input: TestDocumentHbaseMultiClustering): String = {
    implicit val testNestedDocumentToJson = jsonFormat3(TestNestedDocument.apply)
    val testDocumentToJson = jsonFormat4(TestDocumentHbaseMultiClustering.apply)
    val jsonObj = testDocumentToJson.write(input)
    jsonObj.compactPrint
  }
}