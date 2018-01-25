package it.agilelab.bigdata.wasp.consumers.rt.readers

import akka.actor.{Actor, ActorRef}
import akka.camel.{CamelMessage, Consumer}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.KafkaConfigModel


/**
 * Created by Mattia Bertorello on 13/10/15.
 */
class CamelKafkaReader(kafkaConfigModel: KafkaConfigModel, topic: String, groupId: String, actorHolder: ActorRef)
    extends Consumer
    with Logging {
  private val kafkaConnections = kafkaConfigModel.connections.mkString(",") // Why the "," https://github.com/apache/camel/blob/master/components/camel-kafka/src/test/java/org/apache/camel/component/kafka/KafkaComponentTest.java

  private val zookeeperConnections = kafkaConfigModel.zookeeperConnections.getZookeeperConnection()

  private val messageBusURL = s"kafka:$kafkaConnections?topic=$topic&zookeeperConnect=$zookeeperConnections&groupId=$groupId"
  //TODO: hardcoded config, esternalizzare
  messageBusURL + "&consumerTimeoutMs=1000"
  messageBusURL + "&autoCommitEnable=true"
  messageBusURL + "&autoCommitIntervalMs=100"
  //TODO: mancano queste opzioni in camel kafka, perchè? Altre opzioni tutto default?
  /*messageBusURL + "&key.deserializer=org.apache.kafka.common.serialization.StringDeserializer"
  messageBusURL + "&value.deserializer=org.apache.kafka.common.serialization.StringDeserializer"
  messageBusURL + "&partition.assignment.strategy=range"*/



  override def endpointUri: String = messageBusURL

  override def receive: Actor.Receive = {
    case message: CamelMessage => actorHolder ! (topic, message.bodyAs[Array[Byte]])
    case _ => logger.info("Unknown message.")
  }

}
