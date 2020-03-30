package it.agilelab.bigdata.wasp.producers.metrics.kafka.throughput

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorRefFactory, Props}
import com.typesafe.config._
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.bl.{ProducerBL, TopicBL}
import it.agilelab.bigdata.wasp.core.utils.ConfUtils._
import it.agilelab.bigdata.wasp.producers.metrics.kafka.KafkaCheckOffsetsGuardian
import it.agilelab.bigdata.wasp.producers.{ProducerGuardian, StartMainTask}

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.language.implicitConversions

/**
  * @author Eugenio Liso
  */
object KafkaThroughputProducerGuardian {
  val REQUESTS_TIMEOUT = FiniteDuration(5000L, TimeUnit.MILLISECONDS)
}

abstract class KafkaThroughputProducerGuardian[A](env: {val producerBL: ProducerBL; val topicBL: TopicBL},
                                                  producerName: String,
                                                  kafkaOffsetCheckerGuardianFactory: ActorRefFactory => ActorRef,
                                                  requestsTimeout: FiniteDuration
                                                 )
  extends ProducerGuardian(env.asInstanceOf[AnyRef {val producerBL: ProducerBL; val topicBL: TopicBL}], producerName) {

  def this(env: {val producerBL: ProducerBL; val topicBL: TopicBL},
           producerName: String) = this(
    env,
    producerName,
    (context: ActorRefFactory) => Await.result(
      context.actorSelection(WaspSystem.actorSystem / KafkaCheckOffsetsGuardian.name)
        .resolveOne(KafkaThroughputProducerGuardian.REQUESTS_TIMEOUT),
      KafkaThroughputProducerGuardian.REQUESTS_TIMEOUT * 2
    ),
    KafkaThroughputProducerGuardian.REQUESTS_TIMEOUT
  )

  override val name: String = "KafkaThroughputProducerGuardian"

  private var kafkaOffsetCheckerGuardian: ActorRef = _
  private var topicActorsMapping: Map[String, ActorRef] = Map.empty[String, ActorRef]

  override def preStart(): Unit = {
    logger.info(s"Created $name with actorRef: $self")
    kafkaOffsetCheckerGuardian = kafkaOffsetCheckerGuardianFactory.apply(context)
    logger.info(s"Retrieved ref of ${KafkaCheckOffsetsGuardian.name}: ${kafkaOffsetCheckerGuardian.toString()}")
  }

  protected def createActor(kafkaActor: ActorRef,
                            topicToCheck: String,
                            triggerInterval: Long,
                            windowSize: Long,
                            sendMessageEveryXsamples: Int): KafkaThroughputProducerActor[A]

  private def spawnKafkaOffsetCheckerActor(kafkaActor: ActorRef,
                                           topicToCheck: String,
                                           triggerInterval: Long,
                                           windowSize: Long,
                                           sendMessageEveryXsamples: Int): ActorRef = {

    val aRef = context.actorOf(
      Props(
        createActor(
          kafkaActor,
          topicToCheck,
          triggerInterval,
          windowSize,
          sendMessageEveryXsamples
        )
      )
    )
    logger.info(
      s"""Created actor KafkaThroughputProducerActor. It will follow this configuration:
         |kafkaActor: $kafkaActor
         |topicToCheck: $topicToCheck
         |triggerInterval: $triggerInterval
         |windowSize: $windowSize
         |sendMessageEveryXsamples: $sendMessageEveryXsamples
         |""".stripMargin)

    topicActorsMapping += (topicToCheck -> aRef)

    aRef
  }

  /**
    * Method to pick the topics you want to monitor.
    * Default implementation looks for the configuration: wasp.kafkaThroughput.topics
    * which is expected to be a list of conf which should contain the following fields
    * <ul>
    * <li>topicName: The topic name to monitor</li>
    * <li>triggerIntervalMs: how many millis should pass between an offset check and the next (i.e. how frequently kafka is going to be contacted)</li>
    * <li>windowSizeMs: window over the throughput will be calculated</li>
    * <li>sendMessageEvery: every how many offset check a new message should be emitted</li>
    * </ul>
    *
    * @return Right list of topics to monitor, Left error message
    *
    */
  protected def kafkaThroughputConfigs(): Either[String, List[KafkaThroughputConfig]] = {
    for {
      confs <- getConfigList(ConfigFactory.load(), "wasp.kafkaThroughput.topics")
      throughputConf <- sequence(confs.map(KafkaThroughputConfig.fromConfig))
    } yield throughputConf
  }

  override def startChildActors(): Unit = {
    val throughputConfs = kafkaThroughputConfigs()

    throughputConfs match {
      case Left(error) =>
        val msg = s"Cannot initialize KafkaThroughputProducers: $error"
        logger.error(msg)
        throw new RuntimeException(msg)
      case Right(configs) =>
        configs.foreach { c =>
          val actorRef = spawnKafkaOffsetCheckerActor(
            kafkaOffsetCheckerGuardian, c.topicToCheck, c.triggerInterval, c.windowSize, c.sendMessageEvery
          )
          actorRef ! StartMainTask
        }
    }
  }
}
