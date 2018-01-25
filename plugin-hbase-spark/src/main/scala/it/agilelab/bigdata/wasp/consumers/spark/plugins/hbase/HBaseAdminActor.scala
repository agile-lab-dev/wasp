package it.agilelab.bigdata.wasp.consumers.spark.plugins.hbase

import akka.actor.Actor
import it.agilelab.bigdata.wasp.core.logging.Logging



object HBaseAdminActor {

  val name = "HBaseAdminActor"
}

class HBaseAdminActor extends Actor with Logging {

  override def receive: Actor.Receive = {
    case message: Any => logger.error("unknown message: " + message)
  }
}