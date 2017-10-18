package it.agilelab.bigdata.wasp.core.models

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import org.mongodb.scala.bson.BsonObjectId

case class DashboardModel(url: String, needsFilterBox: Boolean)

case class StrategyModel(className: String, configuration: Option[String] = None) {
  def configurationConfig() :Option[Config] = configuration.map(ConfigFactory.parseString)
}

object StrategyModel {
  def create(className: String, configuration: Config): StrategyModel = StrategyModel(className, Some(configuration.root().render(ConfigRenderOptions.concise())))
}

// TODO: move common members from child classes here
trait ProcessingComponentModel

case class LegacyStreamingETLModel(name: String,
                                   inputs: List[ReaderModel],
                                   output: WriterModel,
                                   mlModels: List[MlModelOnlyInfo],
                                   strategy: Option[StrategyModel],
                                   kafkaAccessType: String,
                                   group: String = "default",
                                   var isActive: Boolean = true) extends ProcessingComponentModel

case class StructuredStreamingETLModel(name: String,
                                       inputs: List[ReaderModel],
                                       output: WriterModel,
                                       mlModels: List[MlModelOnlyInfo],
                                       strategy: Option[StrategyModel],
                                       kafkaAccessType: String,
                                       group: String = "default",
                                       var isActive: Boolean = true,
                                       config: Map[String, String]) extends ProcessingComponentModel

case class RTModel(name: String,
                   inputs: List[ReaderModel],
                   var isActive: Boolean = true,
                   strategy: Option[StrategyModel] = None,
                   endpoint: Option[WriterModel] = None) extends ProcessingComponentModel

/**
  * A model for a pipegraph, a processing pipeline abstraction.
  *
  * @param name name of the pipegraph
  * @param description description of the pipegraph
  * @param owner owner of the pipegraph
  * @param isSystem whether the pipegraph is from the WASP system
  * @param creationTime time of creation  of the pipegraph
  * @param legacyStreamingComponents components describing processing built on Spark Legacy Streaming
  * @param structuredStreamingComponents components describing processing built on Spark Structured Streaming
  * @param rtComponents components describing processing built on Akka actors
  * @param dashboard dashboard of the pipegraph
  * @param isActive whether the pipegraph is currently active
  * @param _id id of the pipegraph
  */
case class PipegraphModel(override val name: String,
                          description: String,
                          owner: String,
                          isSystem: Boolean,
                          creationTime: Long,
                          legacyStreamingComponents: List[LegacyStreamingETLModel],
                          structuredStreamingComponents: List[StructuredStreamingETLModel],
                          rtComponents: List[RTModel],
                          dashboard: Option[DashboardModel] = None,
                          var isActive: Boolean = true,
                          _id: Option[BsonObjectId] = None) extends Model

object LegacyStreamingETLModel {
  val KAFKA_ACCESS_TYPE_DIRECT = "direct"
  val KAFKA_ACCESS_TYPE_RECEIVED_BASED = "receiver-based"
}