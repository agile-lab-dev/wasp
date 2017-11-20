
package it.agilelab.bigdata.wasp.consumers.rt

import akka.actor._
import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.consumers.rt.readers.CamelKafkaReader
import it.agilelab.bigdata.wasp.consumers.rt.strategies.StrategyRT
import it.agilelab.bigdata.wasp.consumers.rt.writers.RtWritersManagerActor
import it.agilelab.bigdata.wasp.core.bl.{IndexBL, TopicBL, WebsocketBL}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.{RTModel, ReaderModel, TopicModel, WriterModel}
import it.agilelab.bigdata.wasp.core.utils._

import scala.collection.mutable


case class StartRT()

case class StopRT()

class RTActor(env: {val topicBL: TopicBL; val websocketBL: WebsocketBL; val indexBL: IndexBL},
              rt: RTModel,
              listener: ActorRef)
  extends Actor with Logging {
  val strategy: Option[StrategyRT] = createStrategyRT(rt)
  lazy val kafkaReaders: List[Option[ActorRef]] = {
    rt.inputs.map { input =>
      val topicOpt = env.topicBL.getById(input.endpointId.getValue.toHexString)
      val ref = topicOpt match {
        case Some(topic) => {
          //subscribe(topic.name, self)
          //TODO gestione groupId
          Some(context.actorOf(Props(new CamelKafkaReader(ConfigManager.getKafkaConfig, topic.name, groupId = this.hashCode().toString, self))))
        }
        case None =>
          logger.warn(s"RT ${rt.name} has the input id ${input.endpointId.getValue.toHexString} which does not identify a topic")
          None // Should never happen
      }
      ref
    }
  }

  lazy val epManagerActor: ActorRef = initializeEndpointsManager(rt.endpoint)

  // cache for topic data types
  val topicDataTypes = mutable.Map.empty[String, TopicModel]
  
  def initializeEndpointsManager(endpointsModel: Option[WriterModel]) = {
    context.actorOf(Props(new RtWritersManagerActor(env, endpointsModel)))
  }
  
  def receive: Actor.Receive = {
    case StartRT => {
      epManagerActor
      kafkaReaders
    }
    case StopRT => {
      kafkaReaders.foreach {
        kafkaReader =>
          if (kafkaReader.isDefined) {
            context stop kafkaReader.get
          }
      }
      epManagerActor ! PoisonPill
    }
    case (key: String, data: Array[Byte]) => {
      rt.inputs.foreach { input =>
        
        val topicModel = getTopiModel(input)

        topicModel.topicDataType match {
          case "avro" => {
            val topicSchema = JsonConverter.toString(topicModel.schema.asDocument())
            val jsonMsg = AvroToJsonUtil.avroToJson(data/*, topicSchema*/)
            val outputJson = applyStrategy(key, jsonMsg)
            epManagerActor ! outputJson
          }
          case "json" => {
            val jsonMsg = JsonToByteArrayUtil.byteArrayToJson(data)
            val outputJson = applyStrategy(key, jsonMsg)
            epManagerActor ! outputJson
          }
        }
      }
    }
  }
  
  // returns the topic type corresponding to the input given
  private def getTopiModel(input: ReaderModel): TopicModel = {
    val topicId: String = input.endpointId.getValue.toHexString
    val typeOpt: Option[TopicModel] = topicDataTypes.get(topicId)
    
    typeOpt match {
      case Some(topicDataType) => topicDataType // found in cache, simply return it
      case None => { // not found, get from db, add to cache, return it
        val topicOpt: Option[TopicModel] = env.topicBL.getById(input.endpointId.getValue.toHexString)

        topicDataTypes += topicId -> topicOpt.get

        topicOpt.get
      }
    }
  }

  def applyStrategy(topic: String, data: String): String = strategy match {
    case None => data
    case Some(s) =>
      //TODO: usare campo topic per differenziare strategia a seconda dell'input?
      s.transform(topic, data)
  }

  def createStrategyRT(rt: RTModel): Option[StrategyRT] = rt.strategy match {
    case None => None
    case Some(strategyModel) =>
      val result = Class.forName(strategyModel.className).newInstance().asInstanceOf[StrategyRT]
      result.configuration = strategyModel.configurationConfig() match {
        case None => ConfigFactory.empty()
        case Some(configuration) => configuration
      }
      logger.info("strategyRT: " + result)
      Some(result)
  }

}
