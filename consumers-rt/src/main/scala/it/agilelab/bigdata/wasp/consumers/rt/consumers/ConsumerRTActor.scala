
package it.agilelab.bigdata.wasp.consumers.rt.consumers

import akka.actor._
import it.agilelab.bigdata.wasp.consumers.readers.CamelKafkaReader
import it.agilelab.bigdata.wasp.consumers.strategies.StrategyRT
import it.agilelab.bigdata.wasp.consumers.writers.RtWritersManagerActor
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.bl.{IndexBL, TopicBL, WebsocketBL}
import it.agilelab.bigdata.wasp.core.kafka.WaspKafkaReader
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.models.{RTModel, ReaderModel, WriterModel}
import it.agilelab.bigdata.wasp.core.utils._
import reactivemongo.bson._

import scala.concurrent.Await
import scala.collection.mutable


case class StartRT()

case class StopRT()

class ConsumerRTActor(env: {val topicBL: TopicBL; val websocketBL: WebsocketBL; val indexBL: IndexBL},
                      rt: RTModel,
                      listener: ActorRef)
  extends Actor with ActorLogging {

  val logger = WaspLogger(WaspKafkaReader.getClass.toString)
  val strategy: Option[StrategyRT] = createStrategyRT(rt)
  lazy val kafkaReaders: List[Option[ActorRef]] = {
    rt.inputs.map { input =>
      val topicFut = env.topicBL.getById(input.id.stringify)
      val topicOpt = Await.result(topicFut, WaspSystem.timeout.duration)
      val ref = topicOpt match {
        case Some(topic) => {
          //subscribe(topic.name, self)
          //TODO gestione groupId
          Some(context.actorOf(Props(new CamelKafkaReader(ConfigManager.getKafkaConfig, topic.name, groupId = this.hashCode().toString, self))))
        }
        case None =>
          logger.warn(s"RT ${rt.name} has the input id ${input.id.stringify} which does not identify a topic")
          None // Should never happen
      }
      ref
    }
  }

  lazy val epManagerActor: ActorRef = initializeEndpointsManager(rt.endpoint)

  // cache for topic data types
  val topicDataTypes = mutable.Map.empty[String, String]
  
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
        
        val topicDataType = getTopicDataType(input)
        
        topicDataType match {
          case "avro" => {
            val jsonMsg = AvroToJsonUtil.avroToJson(data)
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
  private def getTopicDataType(input: ReaderModel): String = {
    val topicId = input.id.stringify
    val typeOpt = topicDataTypes.get(topicId)
    
    typeOpt match {
      case Some(topicDataType) => topicDataType // found in cache, simply return it
      case None => { // not found, get from db, add to cache, return it
        val topicFut = env.topicBL.getById(input.id.stringify)
        val topicOpt = Await.result(topicFut, WaspSystem.timeout.duration)
        val topicDataType = topicOpt.get.topicDataType
        
        topicDataTypes += topicId -> topicDataType
        
        topicDataType
      }
    }
  }

  def applyStrategy(topic: String, data: String): String = strategy match {
    case None => data
    case Some(strategy) =>
      //TODO: usare campo topic per differenziare strategia a seconda dell'input?
      strategy.transform(topic, data)
  }

  def createStrategyRT(rt: RTModel): Option[StrategyRT] = rt.strategy match {
    case None => None
    case Some(strategyModel) =>
      val result = Class.forName(strategyModel.className).newInstance().asInstanceOf[StrategyRT]
      result.configuration = strategyModel.configuration match {
        case None => Map[String, Any]()
        case Some(configuration) =>
          implicit def reader = new BSONDocumentReader[Map[String, Any]] {
            def read(bson: BSONDocument) =
              bson.elements.map(tuple =>
                tuple._1 -> (tuple._2 match {
                  case s: BSONString => s.value
                  case b: BSONBoolean => b.value
                  case i: BSONInteger => i.value
                  case l: BSONLong => l.value
                  case d: BSONDouble => d.value
                  case o: Any => o.toString
                })).toMap
          }

          BSON.readDocument[Map[String, Any]](configuration)
      }
      logger.info("strategyRT: " + result)
      Some(result)
  }

}
