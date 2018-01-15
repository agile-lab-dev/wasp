package it.agilelab.bigdata.wasp.core.cluster

import akka.actor.SupervisorStrategy._
import akka.actor._
import akka.pattern.gracefulStop
import it.agilelab.bigdata.wasp.core.WaspEvent.NodeInitialized
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.messages.OutputStreamInitialized
import org.apache.commons.lang3.exception.ExceptionUtils

import scala.concurrent.duration._

abstract class ClusterAwareNodeGuardian extends ClusterAware {
  // customize
  override val supervisorStrategy: SupervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 5, withinTimeRange = 1.minute) {
      case e: Exception =>
        if (sender() != null) {
          sender() ! Left(s"${e.getMessage}\n${ExceptionUtils.getStackTrace(e)}")
          log.error(s"The actor ${self.path.address} throw an exception ${e.getMessage}\n${ExceptionUtils.getStackTrace(e)}")
        }
        Restart
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
