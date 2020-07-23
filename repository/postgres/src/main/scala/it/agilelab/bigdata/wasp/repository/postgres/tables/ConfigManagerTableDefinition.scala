package it.agilelab.bigdata.wasp.repository.postgres.tables

import java.sql.ResultSet

import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.models.Model
import it.agilelab.bigdata.wasp.models.configuration.{CompilerConfigModel, ElasticConfigModel, HBaseConfigModel, JdbcConfigModel, KafkaConfigModel, NifiConfigModel, SolrConfigModel, SparkBatchConfigModel, SparkStreamingConfigModel, TelemetryConfigModel}
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

object ConfigManagerTableDefinition extends SimpleModelTableDefinition[Model] with JsonSupport{


  override def tableName: String = "CONFIG_MANAGER"

  def getPayload = payload

  override protected def fromModelToJson(m: Model): JsValue = {
    m match {
      case model: KafkaConfigModel if m.name.equals(ConfigManager.kafkaConfigName) => model.toJson
      case model: SparkBatchConfigModel if m.name.equals(ConfigManager.sparkBatchConfigName) => model.toJson
      case model: SparkStreamingConfigModel if m.name.equals(ConfigManager.sparkStreamingConfigName) => model.toJson
      case model: ElasticConfigModel if m.name.equals(ConfigManager.elasticConfigName) => model.toJson
      case model: SolrConfigModel if m.name.equals(ConfigManager.solrConfigName) => model.toJson
      case model: HBaseConfigModel if m.name.equals(ConfigManager.hbaseConfigName) => model.toJson
      case model: JdbcConfigModel if m.name.equals(ConfigManager.jdbcConfigName) => model.toJson
      case model: TelemetryConfigModel if m.name.equals(ConfigManager.telemetryConfigName) => model.toJson
      case model: NifiConfigModel if m.name.equals(ConfigManager.nifiConfigName) => model.toJson
      case model: CompilerConfigModel if m.name.equals(ConfigManager.compilerConfigName) => model.toJson
      case z: Model => throw new Exception(s"${z.getClass} not supported.")
    }
  }

  override protected def fromJsonToModel(json: JsValue): Model = ??? //not used


  override val from: ResultSet => Model = rs=> {
    val json = rs.getString(payload).parseJson
    val c = ConfigManager.kafkaConfigName
    rs.getString(name) match {
      case ConfigManager.kafkaConfigName => json.convertTo[KafkaConfigModel]
      case ConfigManager.sparkBatchConfigName => json.convertTo[SparkBatchConfigModel]
      case ConfigManager.sparkStreamingConfigName => json.convertTo[SparkStreamingConfigModel]
      case ConfigManager.elasticConfigName => json.convertTo[ElasticConfigModel]
      case ConfigManager.solrConfigName => json.convertTo[SolrConfigModel]
      case ConfigManager.hbaseConfigName => json.convertTo[HBaseConfigModel]
      case ConfigManager.jdbcConfigName => json.convertTo[JdbcConfigModel]
      case ConfigManager.telemetryConfigName => json.convertTo[TelemetryConfigModel]
      case ConfigManager.nifiConfigName => json.convertTo[NifiConfigModel]
      case ConfigManager.compilerConfigName => json.convertTo[CompilerConfigModel]
      case t => throw new Exception(s"$t not supported")
    }
  }



}
