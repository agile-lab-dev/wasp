package it.agilelab.bigdata.wasp.master

import java.util.Calendar

import akka.actor.{Actor, ActorRef, PoisonPill, Props, actorRef2Scala}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe
import akka.pattern.ask
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.{??, actorSystem, synchronousActorCallTimeout}
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.cluster.ClusterAwareNodeGuardian
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.messages._
import it.agilelab.bigdata.wasp.core.models.{BatchJobModel, PipegraphModel, ProducerModel}
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, WaspConfiguration}

import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, DurationInt, HOURS, MILLISECONDS}


object MasterGuardian extends WaspConfiguration {
  lazy val logger = WaspLogger(this.getClass.getName)
  
  val name = "MasterGuardian"
  
  val role = "master"
  
  // get distributed publish-subscribe mediator
  val mediator = DistributedPubSub.get(WaspSystem.actorSystem).mediator

  if (WaspSystem.masterActor == null)
    WaspSystem.masterActor = actorSystem.actorOf(Props(new MasterGuardian(ConfigBL)))

  
  
  // at midnight, restart all active pipelines (this causes new timed indices creation and consumers redirection on new indices)
  if (ConfigManager.getWaspConfig.indexRollover) {
    val timeToFirst = ceilDayTime(System.currentTimeMillis) - System.currentTimeMillis
    val initialDelay = Duration(timeToFirst, MILLISECONDS)
    val interval = Duration(24, HOURS)
    logger.info(f"Index rollover is enabled: scheduling index rollover ${initialDelay.toUnit(HOURS)}%4.2f hours from now and then every $interval")
    actorSystem.scheduler.schedule(initialDelay, interval) {
      ??[Boolean](WaspSystem.masterActor, RestartPipegraphs)
    }
  } else {
    logger.info("Index rollover is disabled.")
  }

  // TODO use cluster singleton proxy to reach them
  var consumer: ActorRef = _ //findLocallyOrCreateActor(Props(new ConsumersMasterGuardian(ConfigBL, writers.SparkWriterFactoryDefault, KafkaReader)), ConsumersMasterGuardian.name)
  var batchGuardian: ActorRef = _ //findLocallyOrCreateActor(Props(new batch.BatchMasterGuardian(ConfigBL, None, writers.SparkWriterFactoryDefault)), batch.BatchMasterGuardian.name)

  // TODO configurable timeout
  def findLocallyOrCreateActor(props: Props, name: String): ActorRef = {
    try {
      val actorPath = "/user/" + name
      val actorSelection = actorSystem.actorSelection(actorPath)
      logger.info(s"Attempting actor selection: $actorSelection")
      val actor = Await.result(actorSelection.resolveOne()(synchronousActorCallTimeout), ConfigManager.getWaspConfig.generalTimeoutMillis milliseconds)
      logger.info(s"Selected actor: $actor")
      actor
    } catch {
      case e: Exception => {
        logger.info("Failed actor selection! Creating actor.")
        val actor = actorSystem.actorOf(props, name)
        logger.info(s"Created actor: $actor")
        actor
      }
    }
  }
  
  private def ceilDayTime(time: Long): Long = {

    val cal = Calendar.getInstance()
    cal.setTimeInMillis(time)
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    cal.add(Calendar.DATE, 1)
    cal.getTime.getTime
  }
}

class MasterGuardian(env: {val producerBL: ProducerBL; val pipegraphBL: PipegraphBL; val batchJobBL: BatchJobBL; val batchSchedulerBL: BatchSchedulersBL; }, val classLoader: Option[ClassLoader] = None) extends ClusterAwareNodeGuardian {
  
  import MasterGuardian._
  
  lazy val logger = WaspLogger(this.getClass.getName)
  
  // TODO just for Class Loader debug.
  // logger.error("Framework ClassLoader"+this.getClass.getClassLoader.toString())
  
  // subscribe to producers topic using distributed publish subscribe
  mediator ! Subscribe(WaspSystem.producersPubSubTopic, self)
  
  // all producers, whether they are local or remote
  def producers: Map[String, ActorRef] = localProducers ++ remoteProducers
  
  // local producers, which run in the same JVM as the MasterGuardian (those with isRemote = false)
  val localProducers: Map[String, ActorRef] = {
    env.producerBL
      .getAll // grab all producers
      .filterNot(_.isRemote) // filter only local ones
      .map(producer => {
      val producerId = producer._id.get.getValue.toHexString
      if (producer.name == "LoggerProducer") { // logger producer is special
        producerId -> WaspSystem.loggerActor.get // do not instantiate, but get the already existing one from WaspSystem
      } else {
        val producerClass = classLoader.map(cl => cl.loadClass(producer.className)).getOrElse(Class.forName(producer.className))
        val producerActor = actorSystem.actorOf(Props(producerClass, ConfigBL, producerId), producer.name)
        producerId -> producerActor
      }
    }).toMap
  }
  
  // remote producers, which run in a different JVM than the MasterGuardian (those with isRemote = true)
  val remoteProducers: mutable.Map[String, ActorRef] = mutable.Map.empty[String, ActorRef]
  
  // on startup non-system pipegraphs and associated consumers are deactivated
  setPipegraphsActive(env.pipegraphBL.getSystemPipegraphs(false), isActive = false)
  logger.info("Deactivated non-system pipegraphs")
  
  // on startup all producers are deactivated
  setProducersActive(env.producerBL.getActiveProducers(), isActive = false)
  logger.info("Deactivated producers")
  
  
  private def setPipegraphsActive(pipegraphs: Seq[PipegraphModel], isActive: Boolean): Unit = {
    pipegraphs.foreach(pipegraph => env.pipegraphBL.setIsActive(pipegraph, isActive))
  }
  
  private def setProducersActive(producers: Seq[ProducerModel], isActive: Boolean): Unit = {
    producers.foreach(producer => env.producerBL.setIsActive(producer, isActive))
  }
  
  // TODO manage error in pipegraph initialization
  // auto-start raw pipegraph
  if (waspConfig.systemPipegraphsStart) {
    env.pipegraphBL.getByName("RawPipegraph") match {
      case None => logger.error("RawPipegraph not found")
      case Some(pipegraph) => self.actorRef ! StartPipegraph(pipegraph._id.get.getValue.toHexString)
    }
  
    // auto-start logger pipegraph
    env.pipegraphBL.getByName("LoggerPipegraph") match {
      case None => logger.error("LoggerPipegraph not found")
      case Some(pipegraph) => self.actorRef ! StartPipegraph(pipegraph._id.get.getValue.toHexString)
    }
  } else {
    setPipegraphsActive(env.pipegraphBL.getSystemPipegraphs(), isActive = false)
  }
  
  logger.info("Batch schedulers initializing ...")
  batchGuardian ! StartSchedulersMessage()
  
  // TODO try without sender parenthesis
  def initialized: Actor.Receive = {
    //case message: RemovePipegraph          => call(message, onPipegraph(message.id, removePipegraph))
    case message: StartPipegraph => call(sender(), message, onPipegraph(message.id, startPipegraph))
    case message: StopPipegraph => call(sender(), message, onPipegraph(message.id, stopPipegraph))
    case RestartPipegraphs => call(sender(), RestartPipegraphs, onRestartPipegraphs())
    case message: AddRemoteProducer => call(message.remoteProducer, message, onProducer(message.id, addRemoteProducer(message.remoteProducer, _))) // do not use sender() for actor ref: https://github.com/akka/akka/issues/17977
    case message: RemoveRemoteProducer => call(message.remoteProducer, message, onProducer(message.id, removeRemoteProducer(message.remoteProducer, _))) // do not use sender() for actor ref: https://github.com/akka/akka/issues/17977
    case message: StartProducer => call(sender(), message, onProducer(message.id, startProducer))
    case message: StopProducer => call(sender(), message, onProducer(message.id, stopProducer))
    case message: StartETL => call(sender(), message, onEtl(message.id, message.etlName, startEtl))
    case message: StopETL => call(sender(), message, onEtl(message.id, message.etlName, stopEtl))
    case message: StartBatchJob => call(sender(), message, onBatchJob(message.id, startBatchJob))
    case message: StartPendingBatchJobs => call(sender(), message, startPendingBatchJobs())
    case message: BatchJobProcessedMessage => //TODO gestione batchJob finito?
    //case message: Any                     => logger.error("unknown message: " + message)
  }
  
  private def call[T <: MasterGuardianMessage](sender: ActorRef, message: T, future: Future[Either[String, String]]) = {
    future.map(result => {
      logger.info(message + ": " + result)
      sender ! result
    })
  }
  
  private def onPipegraph(id: String, f: PipegraphModel => Future[Either[String, String]]) = {
    env.pipegraphBL.getById(id) match {
      case None => Future(Right("Pipegraph not retrieved"))
      case Some(pipegraph) => f(pipegraph)
    }
  }
  
  private def onRestartPipegraphs(): Future[Either[String, String]] = {
    consumer ! RestartConsumers
    Future(Left("Pipegraphs restart started."))
  }
  
  private def onProducer(id: String, f: ProducerModel => Future[Either[String, String]]) = {
    env.producerBL.getById(id) match {
      case None => Future(Right("Producer not retrieved"))
      case Some(producer) => f(producer)
    }
  }
  
  
  private def onEtl(idPipegraph: String, etlName: String, f: (PipegraphModel, String) => Future[Either[String, String]]) = {
    env.pipegraphBL.getById(idPipegraph) match {
      case None => Future(Right("ETL not retrieved"))
      case Some(pipegraph) => f(pipegraph, etlName)
    }
  }

  private def onBatchJob(id: String, f: BatchJobModel => Future[Either[String, String]]) = {
    env.batchJobBL.getById(id) match {
      case None => Future(Right("BatchJob not retrieved"))
      case Some(batchJob) => f(batchJob)
    }
  }

  private def manageThrowable(message: String, throwable: Throwable): String = {
    var result = message
    logger.error(message, throwable)

    if (throwable.getMessage != null && !throwable.getMessage.isEmpty)
      result = result + " Cause: " + throwable.getMessage

    result
  }

  private def cleanPipegraph(message: RemovePipegraph) = {
    //sender ! true
  }

  private def startPipegraph(pipegraph: PipegraphModel) = {
    // persist pipegraph as active
    setActiveAndRestart(pipegraph, active = true)
  }

  private def stopPipegraph(pipegraph: PipegraphModel): Future[Either[String, String]] = {
    setActiveAndRestart(pipegraph, active = false)
  }

  private def setActiveAndRestart(pipegraph: PipegraphModel, active: Boolean): Future[Either[String, String]] = {
    env.pipegraphBL.setIsActive(pipegraph, isActive = active)
    val res = ??[Boolean](consumer, RestartConsumers, Some(WaspSystem.synchronousActorCallTimeout.duration))

    if (res) {
      Future(Right("Pipegraph '" + pipegraph.name + "' " + (if (active) "started" else "stopped")))
    } else {
      env.pipegraphBL.setIsActive(pipegraph, isActive = !active)
      Future(Left("Pipegraph '" + pipegraph.name + "' not " + (if (active) "started" else "stopped")))
    }
  }

  //TODO  implementare questa parte
  private def startEtl(pipegraph: PipegraphModel, etlName: String): Future[Either[String, String]] = {
    Future(Right("ETL '" + etlName + "' not started"))
  }

  //TODO  implementare questa parte
  private def stopEtl(pipegraph: PipegraphModel, etlName: String): Future[Either[String, String]] = {
    Future(Right("ETL '" + etlName + "' not stopped"))
  }
  
  private def addRemoteProducer(producerActor: ActorRef, producerModel: ProducerModel): Future[Either[String, String]] = {
    val producerId = producerModel._id.get.getValue.toHexString
    if (remoteProducers.isDefinedAt(producerId)) { // already added
      Future(Right(s"Remote producer $producerId ($producerActor) not added; already present."))
    } else { // add to remote producers & start if needed
      remoteProducers += producerId -> producerActor
      if (producerModel.isActive) {
        self ! StartProducer(producerId)
      }
      Future(Left(s"Remote producer $producerId ($producerActor) added."))
    }
  }
  
  private def removeRemoteProducer(producerActor: ActorRef, producerModel: ProducerModel): Future[Either[String, String]] = {
    val producerId = producerModel._id.get.getValue.toHexString
    if (remoteProducers.isDefinedAt(producerId)) { // found, remove
      val producerActor = remoteProducers(producerId)
      remoteProducers.remove(producerId)
      Future(Left(s"Remote producer $producerId ($producerActor) removed."))
    } else { // not found
      Future(Right(s"Remote producer $producerId not found; either it was never added or it has already been removed."))
    }
  }

  private def startProducer(producer: ProducerModel): Future[Either[String, String]] = {
    // initialise producer actor if not already present
    if (producers.isDefinedAt(producer._id.get.getValue.toHexString)) {
      if (! ??[Boolean](producers(producer._id.get.getValue.toHexString), Start)) {
        Future(Right(s"Producer '${producer.name}' not started"))
      } else {
        Future(Left(s"Producer '${producer.name}' started"))
      }
    } else {
      Future(Right(s"Producer '${producer.name}' not exists"))
    }

  }

  private def stopProducer(producer: ProducerModel): Future[Either[String, String]] = {
    if (!producers.isDefinedAt(producer._id.get.getValue.toHexString)) {
      Future(Right("Producer '" + producer.name + "' not initializated"))
    } else if (! ??[Boolean](producers(producer._id.get.getValue.toHexString), Stop)) {
      Future(Right("Producer '" + producer.name + "' not stopped"))
    } else {
      Future(Left("Producer '" + producer.name + "' stopped"))
    }
  }

  private def startBatchJob(batchJob: BatchJobModel): Future[Either[String, String]] = {
    logger.info(s"Send the message StartBatchJobMessage to batchGuardian: job to start: ${batchJob._id.get.getValue.toHexString}")
    implicit val timeut = synchronousActorCallTimeout
    val jobFut = batchGuardian ? StartBatchJobMessage(batchJob._id.get.getValue.toHexString)
    val jobRes = Await.result(jobFut, WaspSystem.synchronousActorCallTimeout.duration).asInstanceOf[BatchJobResult]
    if (jobRes.result) {
      Future(Left("Batch job '" + batchJob.name + "' queued or processing"))
    } else {
      Future(Right("Batch job '" + batchJob.name + "' not processing"))
    }
  }

  private def startPendingBatchJobs(): Future[Either[String, String]] = {
    //TODO: delete log
    logger.info("Sending CheckJobsBucketMessage to Batch Guardian.")
    batchGuardian ! CheckJobsBucketMessage()
    Future(Left("Batch jobs checking started"))
  }

  override def postStop() {
    WaspSystem.elasticAdminActor ! PoisonPill
    WaspSystem.getKafkaAdminActor ! PoisonPill
    WaspSystem.solrAdminActor ! PoisonPill
    consumer ! PoisonPill
    batchGuardian ! PoisonPill

    super.postStop()
  }
}
