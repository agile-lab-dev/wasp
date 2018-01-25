package it.agilelab.bigdata.wasp.producers

// TODO update this for new akka/akka http/akka streams version

/*
import akka.actor._
import akka.http.Http
import akka.http.model.{HttpRequest, HttpResponse}
import akka.io.IO
import akka.pattern.ask
import akka.routing.BalancingPool
import akka.stream.FlowMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import it.agilelab.bigdata.wasp.core.WaspSystem.actorSystem
import it.agilelab.bigdata.wasp.core.bl.{ProducerBL, TopicBL}
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import reactivemongo.bson.{BSONInteger, BSONString}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


abstract class HttpProducer(env: {val producerBL: ProducerBL; val topicBL: TopicBL}, producerId: String) extends ProducerMasterGuardian(env, producerId) {

  val name = "HttpProducer"
  implicit val askTimeout: Timeout = 1000.millis
  implicit val materializer = FlowMaterializer()(actorSystem)

  override def initialize(): Unit = {

    val producerFuture = env.producerBL.getByName(name)

    producerFuture map {
      p => {
        if (p.isDefined) {
          producer = p.get
          //TODO  mergiare con codice classe base
          if (producer.hasOutput) {
            val topicFuture = env.producerBL.getTopic(env.topicBL, producer)
            topicFuture map {
              topic => {
                associatedTopic = topic
                kafka_router = actorSystem.actorOf(BalancingPool(5).props(Props(new KafkaPublisherActor(ConfigManager.getKafkaConfig))), router_name)

                context become initialized
                startChildActors()

                env.producerBL.setIsActive(producer, isActive = true)
              }
            }
          } else {
            logger.info("This producer hasn't associated topic")
          }
        } else {
          logger.info("Unable to fecth producer")
        }
      }

    }

  }


  def requestHandler: HttpRequest ⇒ HttpResponse

  def initHttpServer() {

    val address = producer.configuration.flatMap(bson => bson.getAs[BSONString]("address").map(_.seeAsOpt[String].get)).getOrElse("localhost")
    val port = producer.configuration.flatMap(bson => bson.getAs[BSONInteger]("port").map(_.seeAsOpt[Int].get)).getOrElse(scala.util.Random.nextInt(7000) + 10000)

    logger.info(s"Starting HttpProducer server on address [$address] and port [$port]")
    val serverBinding = IO(Http) ? Http.Bind(address, port)

    serverBinding foreach {
      case Http.ServerBinding(localAddress, connectionStream) ⇒
        Source(connectionStream).foreach({
          case Http.IncomingConnection(remoteAddress, requestProducer, responseConsumer) ⇒
            logger.info("Accepted new connection from " + remoteAddress)

            Source(requestProducer).map(requestHandler).to(Sink(responseConsumer)).run()
        })
    }


  }

  override def postStop() {
    // Do we need to kill the listening server?
    //IO(Http) ! PoisonPill
  }

}
*/