package it.agilelab.bigdata.wasp.producers.metrics.kafka.backlog

import java.util.concurrent.TimeUnit
import akka.actor.{ActorRef, ActorRefFactory, Cancellable, Props}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe
import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.repository.core.bl.{ConfigBL, ProducerBL, TopicBL}
import it.agilelab.bigdata.wasp.core.consumers.BaseConsumersMasterGuadian
import it.agilelab.bigdata.wasp.core.messages._
import it.agilelab.bigdata.wasp.core.utils.ConfUtils._
import it.agilelab.bigdata.wasp.models.{DatastoreModel, MultiTopicModel, PipegraphModel, TopicModel}
import it.agilelab.bigdata.wasp.producers.ProducerGuardian
import it.agilelab.bigdata.wasp.producers.metrics.kafka.{KafkaCheckOffsetsGuardian, KafkaOffsetActorAlive}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContextExecutor}

object BacklogSizeAnalyzerProducerGuardian {
  val REQUESTS_TIMEOUT: FiniteDuration = FiniteDuration(5, TimeUnit.SECONDS)
}

/**
  * @author Eugenio Liso, Antonio Murgia
  */
abstract class BacklogSizeAnalyzerProducerGuardian[A](env: { val producerBL: ProducerBL; val topicBL: TopicBL }, producerName: String, kafkaOffsetCheckerGuardianFactory: ActorRefFactory => ActorRef, requestsTimeout: FiniteDuration)
    extends ProducerGuardian(env, producerName) {

  def this(env: { val producerBL: ProducerBL; val topicBL: TopicBL }, producerName: String) = {
    this(
      env,
      producerName,
      factory =>
        Await.result(
          factory
            .actorSelection(WaspSystem.actorSystem / KafkaCheckOffsetsGuardian.name)
            .resolveOne(BacklogSizeAnalyzerProducerGuardian.REQUESTS_TIMEOUT),
          BacklogSizeAnalyzerProducerGuardian.REQUESTS_TIMEOUT * 2
        ),
      BacklogSizeAnalyzerProducerGuardian.REQUESTS_TIMEOUT
    )
  }

  override val name: String = "BacklogSizeAnalyzerProducerGuardian"

  private val mediator               = DistributedPubSub(context.system).mediator
  private val REQUESTS_TIMEOUT: Long = 5000

  private var pipegraphActorsMapping: Map[String, Map[String, ActorRef]] = Map.empty[String, Map[String, ActorRef]]
  private var kafkaOffsetCheckerActor: ActorRef                          = _

  protected def createActor(
      kafka_router: ActorRef,
      kafkaOffsetChecker: ActorRef,
      topic: Option[TopicModel],
      topicToCheck: String,
      etlName: String
  ): BacklogSizeAnalyzerProducerActor[A]

  override def preStart(): Unit = {
    super.preStart()
    logger.info(s"Created $name with actorRef: $self")
    // TODO we don't wait for SubscribeAck but nothing starts if we don't get data, so I think it's not a big deal
    mediator ! Subscribe(WaspSystem.telemetryPubSubTopic, self)
    kafkaOffsetCheckerActor = kafkaOffsetCheckerGuardianFactory.apply(context)
    logger.info(s"Retrieved ref of ${KafkaCheckOffsetsGuardian.name}: ${kafkaOffsetCheckerActor.toString()}")
  }

  private def sendPeriodicTimeoutMessage(): Cancellable = {

    implicit val executionContext: ExecutionContextExecutor = context.system.dispatcher

    val interval = FiniteDuration(REQUESTS_TIMEOUT, TimeUnit.MILLISECONDS)

    val cancellable = context.system.scheduler.schedule(
      interval,
      interval,
      self,
      MessageTimeout
    )

    cancellable

  }

  private def sendMessageKafkaOffsetActor(): Unit = kafkaOffsetCheckerActor ! KafkaOffsetActorAlive

  override def startChildActors(): Unit = {
    //This is the first method called by the WASP framework
    //The first step is to ensure that we can communicate with the KafkaOffsetChecker Actor
    val cancellableKafka = sendPeriodicTimeoutMessage()
    logger.debug("Send first message to kafka offsetActor")
    sendMessageKafkaOffsetActor()
    logger.debug("Start waiting for KafkaOffsetActor")
    context become waitingForKafkaOffsetActor(cancellableKafka)
    logger.debug("Waiting for alive response")
  }

  /**
    * Method to pick the pipegraphs you want to monitor.
    * Default implementation looks for the configuration: wasp.backlogSizeAnalyzerConfig.pipegraphs
    * which is expected to be a list of conf which should contain a field named {{{pipegraphName}}} which contains the
    * pipegraph name
    *
    * @param allPipegraphs map containing all pipegraphs currently available in Wasp
    * @return Right list of pipegraphs to monitor, Left error message
    *
    */
  protected def backlogAnalyzerConfigs(
      allPipegraphs: Map[String, PipegraphModel]
  ): Either[String, List[BacklogAnalyzerConfig]] = {
    for {
      configs                <- getConfigList(ConfigFactory.load(), "wasp.backlogSizeAnalyzer.pipegraphs")
      backlogAnalyzerConfigs <- sequence(configs.map(BacklogAnalyzerConfig.fromConfig(_, allPipegraphs)))
    } yield backlogAnalyzerConfigs
  }

  protected def getAllPipegraphs: Map[String, PipegraphModel] =
    ConfigBL.pipegraphBL.getAll.map(p => p.name -> p).toMap

  private def startChildActorsWhenReady(): Unit = {
    val pipegraphsStructStreaming: Map[String, PipegraphModel] = getAllPipegraphs

    backlogAnalyzerConfigs(pipegraphsStructStreaming) match {
      case Right(confs) =>
        confs.foreach { conf =>
          conf.etls.foreach { etl =>
            val topicName  = etl.streamingInput.datastoreModelName
            val mappingKey = BaseConsumersMasterGuadian.generateUniqueComponentName(conf.pipegraph, etl)

            /*
            We need to extract topic NAMES from topic MODEL since if we're dealing with a [[MultiTopicModel]]
            it can aggregate >= 1 topics and we need to spawn a child actor for each inner topic
             */
            val topicNames = extractTopicNames(env.topicBL, topicName)
            topicNames.foreach { name =>
              spawnChildActor(mappingKey, name, etl.name)
              logger.info(s"Created BacklogSizeAnalyzer Actor for $name and ETL: $mappingKey")
            }
          }
        }
      case Left(error) =>
        val msg = s"Cannot initialize KafkaThroughputProducers: $error"
        logger.error(msg)
        throw new RuntimeException(msg)
    }
  }

  private def extractTopicNames(topicBL: TopicBL, topicModelName: String): Seq[String] = {
    val model: Option[DatastoreModel] = topicBL.getByName(topicModelName)

    if (model.isDefined) {
      model.get match {
        case topic: TopicModel =>
          Seq(topic.name)
        case multiTopic: MultiTopicModel =>
          multiTopic.topicModelNames
      }
    } else {
      Seq.empty[String]
    }
  }

  private def spawnChildActor(mappingKey: String, topicToCheck: String, etlName: String): Unit = {
    val aRef: ActorRef = context.actorOf(
      Props(
        createActor(kafka_router, kafkaOffsetCheckerActor, associatedTopic, topicToCheck, etlName)
      )
    )
    val existingActorMap = pipegraphActorsMapping.getOrElse(mappingKey, Map.empty[String, ActorRef])
    val newActorMap      = existingActorMap + (topicToCheck -> aRef)
    pipegraphActorsMapping = pipegraphActorsMapping + (mappingKey -> newActorMap)
  }

  private def waitingForKafkaOffsetActor(cancellable: Cancellable): Receive =
    waitingForKafkaOffsetActorR(cancellable).orElse(initialized)

  private def waitingForKafkaOffsetActorR(cancellable: Cancellable): Receive = {
    case MessageTimeout =>
      logger.warn(
        "The actor KafkaOffsetCheckerProducerGuardian is not ready to handle the offsets requests. " +
          "Make sure it is started."
      )
      sendMessageKafkaOffsetActor()

    case KafkaOffsetActorAlive =>
      logger.info(
        "Successfully connected to the KafkaOffsetCheckerProducerGuardian actor. " +
          "From now on, the actor will be used to fetch the Kafka offsets."
      )

      cancellable.cancel()
      startChildActorsWhenReady()
      context become waitingForTelemetryMessage
  }

  def waitingForTelemetryMessage: Receive = waitingForTelemetryMessageR.orElse(initialized)

  private val waitingForTelemetryMessageR: Receive = {
    case TelemetryActorRedirection(telemetryActorRef) =>
      logger.debug(
        s"Successfully connected to the telemetry actor with actorRef: $telemetryActorRef. " +
          "From now on, messages will be sent also to this actor."
      )

      telemetryActorRef ! TelemetryActorRedirection(self)

    case data: TelemetryMessageSourcesSummary =>
      logger.debug(s"Sending telemetry info $data to child")
      sendTelemetryInfoToChild(data)

  }

  private def sendTelemetryInfoToChild(data: TelemetryMessageSourcesSummary): Unit = {

    val pipegraphUniqueNames: Seq[String] = data.streamingQueriesProgress.map(_.sourceId)
    val involvedTopics: Seq[String]       = data.streamingQueriesProgress.flatMap(_.startOffset.keySet)

    pipegraphUniqueNames.foreach { pipegraphUniqueName =>
      // If this actor receives a message for a pipegraph that it is not monitoring,
      // ignore the message. Otherwise send the message to the children
      // (we can have more children in case of multi topic model)
      if (pipegraphActorsMapping.contains(pipegraphUniqueName)) {
        val allChildrenRefs = pipegraphActorsMapping(pipegraphUniqueName)
        allChildrenRefs.foreach {
          case (topicName, aRef) if involvedTopics.contains(topicName) =>
            aRef ! data
            logger.debug(s"Sent data: $data to actor $aRef")
          case _ =>
        }
      } else {
        logger.trace(s"Received data: $data but there is no actor monitoring it.")
      }
    }
  }
}
