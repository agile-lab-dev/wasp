package it.agilelab.bigdata.wasp.core.hbase

import akka.actor.{Actor, actorRef2Scala}
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.models.configuration.ElasticConfigModel
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress

object HBaseAdminActor {

  val name = "HBaseAdminActor"
}

class HBaseAdminActor extends Actor {

  val logger = WaspLogger(classOf[HBaseAdminActor])

  def receive: Actor.Receive = {

    case message: Any => logger.error("unknown message: " + message)
  }
}