package it.agilelab.bigdata.wasp.core.cluster

import java.util.concurrent.TimeoutException

import akka.actor._
import akka.actor.SupervisorStrategy._
import akka.pattern.gracefulStop
import akka.util.Timeout

import scala.concurrent.duration._
import it.agilelab.bigdata.wasp.core.WaspEvent.OutputStreamInitialized
import it.agilelab.bigdata.wasp.core.WaspEvent.NodeInitialized
import it.agilelab.bigdata.wasp.core.WaspSystem

abstract class ClusterAwareNodeGuardian extends ClusterAware {
  // customize
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minute) {
      case _: ActorInitializationException => Stop
      case _: IllegalArgumentException => Stop
      case _: IllegalStateException => Restart
      case _: TimeoutException => Escalate
      case _: Exception => Escalate
    }

  override def preStart(): Unit = {
    super.preStart()
    log.info("Starting at {}", cluster.selfAddress)
  }

  override def postStop(): Unit = {
    super.postStop()
    log.info("Node {} shutting down.", cluster.selfAddress)
    cluster.leave(self.path.address)
    gracefulShutdown()
  }

  override def receive: Actor.Receive = uninitialized orElse initialized orElse super.receive


  def uninitialized: Actor.Receive = {
    case OutputStreamInitialized => initialize()
  }

  def initialize(): Unit = {
    log.info(s"Node is transitioning from 'uninitialized' to 'initialized'")
    context.system.eventStream.publish(NodeInitialized)
  }

  def initialized: Actor.Receive

  def gracefulShutdown(): Unit = {
    context.children foreach (gracefulStop(_, WaspSystem.generalTimeout.duration))
    log.info(s"Graceful shutdown completed.")
  }
}
