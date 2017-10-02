package it.agilelab.bigdata.wasp.core.models

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import org.mongodb.scala.bson.{BsonDocument, BsonObjectId}


case class DashboardModel(url: String, needsFilterBox: Boolean)

case class StrategyModel(className: String, configuration: Option[String] = None) {
  def configurationConfig() :Option[Config] = configuration.map(ConfigFactory.parseString)
}

object StrategyModel {
  def create(className: String, configuration: Config): StrategyModel = StrategyModel(className, Some(configuration.root().render(ConfigRenderOptions.concise())))
}

case class ETLModel(name: String, inputs: List[ReaderModel], output: WriterModel, mlModels: List[MlModelOnlyInfo], strategy: Option[StrategyModel], kafkaAccessType: String, group: String = "default", var isActive: Boolean = true)

case class ETLStructuredModel(name: String, inputs: List[ReaderModel], output: WriterModel, mlModels: List[MlModelOnlyInfo], strategy: Option[StrategyModel], kafkaAccessType: String, group: String = "default", var isActive: Boolean = true)

case class RTModel(name: String, inputs: List[ReaderModel], var isActive: Boolean = true, strategy: Option[StrategyModel] = None, endpoint: Option[WriterModel] = None)

case class PipegraphModel(override val name: String,
                          description: String,
                          owner: String,
                          isSystem: Boolean,
                          creationTime: Long,
                          etl: List[ETLModel],
                          etlStructured: List[ETLStructuredModel],
                          rt: List[RTModel],
                          dashboard: Option[DashboardModel] = None,
                          var isActive: Boolean = true,
                          _id: Option[BsonObjectId] = None) extends Model

object ETLModel {
  val KAFKA_ACCESS_TYPE_DIRECT = "direct"
  val KAFKA_ACCESS_TYPE_RECEIVED_BASED = "receiver-based"
}