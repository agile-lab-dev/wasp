package it.agilelab.bigdata.wasp.whitelabel.producers.test

import java.util.UUID

import akka.actor.{ActorRef, Props}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import it.agilelab.bigdata.wasp.core.bl.{ProducerBL, TopicBL}
import it.agilelab.bigdata.wasp.core.models.{MetadataModel, PathModel, TopicModel}
import it.agilelab.bigdata.wasp.producers.{ProducerActor, ProducerGuardian, StartMainTask}
import it.agilelab.bigdata.wasp.whitelabel.models.test.{TestDocument, TestDocumentWithMetadata, TestNestedDocument}
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.concurrent.ExecutionContext

final class TestDocumentWithMetadataProducerGuardian(env: {val producerBL: ProducerBL; val topicBL: TopicBL}, producerName: String)
  extends ProducerGuardian(env, producerName) {

  override val name: String = "testDocumentWithMetadataProducer"

  override def startChildActors(): Unit = {

    val aRef = context.actorOf(Props(new TestDocumentWithMetadataProducerActor(kafka_router, associatedTopic, name)))
    logger.info("Created actor")
    aRef ! StartMainTask
  }
}

private[producers] class TestDocumentWithMetadataProducerActor(kafka_router: ActorRef, topic: Option[TopicModel],
                                                               sourceName: String)
  extends ProducerActor[TestDocumentWithMetadata](kafka_router, topic)
    with SprayJsonSupport with DefaultJsonProtocol {


  var documentId = 0

  override def retrievePartitionKey: TestDocumentWithMetadata => String = (td: TestDocumentWithMetadata) => td.id

  override def mainTask(): Unit = {
    logger.info(s"Starting main task for actor: ${this.getClass.getName}")

    val doc = createTestDocument(documentId)
    logger.info(s"TestDocument $documentId CREATED!")
    sendMessage(doc)

    documentId += 1
    import scala.concurrent.duration._
    implicit val executor: ExecutionContext = context.dispatcher
    system.scheduler.scheduleOnce(1.millisecond) {
      self ! StartMainTask
    }
  }

  private def createTestDocument(documentId: Int) = {


    val now = System.currentTimeMillis()
    val metadata = MetadataModel(documentId.toString, sourceName, now, now, Array.empty)

    val nestedDocument = TestNestedDocument("field1_" + documentId, documentId, Some("field3_" +
      documentId))
    TestDocumentWithMetadata(metadata, "" + documentId, documentId, nestedDocument)
  }

  implicit lazy val pathModelToJson: RootJsonFormat[PathModel] = jsonFormat2(PathModel.apply)
  implicit lazy val metadataModelToJson: RootJsonFormat[MetadataModel] = jsonFormat5(MetadataModel.apply)
  implicit lazy val testNestedDocumentToJson: RootJsonFormat[TestNestedDocument] = jsonFormat3(TestNestedDocument.apply)

  override def generateOutputJsonMessage(input: TestDocumentWithMetadata): String = {


    val testDocumentToJson: RootJsonFormat[TestDocumentWithMetadata] = jsonFormat4(TestDocumentWithMetadata.apply)
    val jsonObj = testDocumentToJson.write(input)
    jsonObj.compactPrint
  }
}