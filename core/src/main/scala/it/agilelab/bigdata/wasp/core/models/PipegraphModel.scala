package it.agilelab.bigdata.wasp.core.models

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}

case class DashboardModel(url: String, needsFilterBox: Boolean)

case class StrategyModel(className: String, configuration: Option[String] = None) {
  def configurationConfig() :Option[Config] = configuration.map(ConfigFactory.parseString)
}

object StrategyModel {
  def create(className: String, configuration: Config): StrategyModel = StrategyModel(className, Some(configuration.root().render(ConfigRenderOptions.concise())))
}

// TODO: move common members from child classes here
trait ProcessingComponentModel {
  def name: String
  
  def generateStandardProcessingComponentName: String = this match {
    case ss: StructuredStreamingETLModel => s"structuredstreaming_${ss.name.replace(" ", "_")}"
    case ls: LegacyStreamingETLModel => s"legacystreaming_${ls.name.replace(" ", "_")}"
    case rt: RTModel => s"rt_${rt.name.replace(" ", "_")}"
  }
  
  def generateStandardWriterName: String = this match { // TODO: remove match once output member is moved into ProcessingComponentModel
    case ss: StructuredStreamingETLModel => s"writer_${ss.output.name.replace(" ", "_")}"
    case ls: LegacyStreamingETLModel => s"writer_${ls.output.name.replace(" ", "_")}"
    case rt: RTModel => rt.endpoint match {
      case Some(output) => s"writer_${output.name.replace(" ", "_")}"
      case None => "no_writer"
    }
  }
}

case class LegacyStreamingETLModel(name: String,
                                   inputs: List[ReaderModel],
                                   output: WriterModel,
                                   mlModels: List[MlModelOnlyInfo],
                                   strategy: Option[StrategyModel],
                                   kafkaAccessType: String,
                                   group: String = "default",
                                   var isActive: Boolean = false) extends ProcessingComponentModel

case class StructuredStreamingETLModel(name: String,
                                       inputs: List[ReaderModel],
                                       output: WriterModel,
                                       mlModels: List[MlModelOnlyInfo],
                                       strategy: Option[StrategyModel],
                                       kafkaAccessType: String,
                                       group: String = "default",
                                       var isActive: Boolean = false,
                                       config: Map[String, String]) extends ProcessingComponentModel

case class RTModel(name: String,
                   inputs: List[ReaderModel],
                   var isActive: Boolean = false,
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
                          var isActive: Boolean = false) extends Model {

  def generateStandardPipegraphName: String = s"pipegraph_$name"
  
  def hasSparkComponents: Boolean = legacyStreamingComponents.nonEmpty || structuredStreamingComponents.nonEmpty
  
  def hasRtComponents: Boolean = rtComponents.nonEmpty
}

object LegacyStreamingETLModel {
  val KAFKA_ACCESS_TYPE_DIRECT = "direct"
  val KAFKA_ACCESS_TYPE_RECEIVED_BASED = "receiver-based"
}