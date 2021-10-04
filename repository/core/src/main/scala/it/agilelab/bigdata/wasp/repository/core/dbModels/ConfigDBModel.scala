package it.agilelab.bigdata.wasp.repository.core.dbModels

import it.agilelab.bigdata.wasp.models.Model
import it.agilelab.bigdata.wasp.models.configuration.{ConnectionConfig, HBaseEntryConfig, JdbcConnectionConfig, KafkaEntryConfig, KryoSerializerConfig, NifiStatelessConfigModel, RetainedConfigModel, SchedulingStrategyConfigModel, SparkDriverConfig, SparkEntryConfig, TelemetryTopicConfigModel, ZookeeperConnectionsConfig}

trait ConfigDBModel extends Model

trait SolrConfigDBModel extends ConfigDBModel

trait HBaseConfigDBModel extends ConfigDBModel

trait KafkaConfigDBModel extends ConfigDBModel

trait SparkBatchConfigDBModel extends ConfigDBModel

trait SparkStreamingConfigDBModel extends ConfigDBModel

trait ElasticConfigDBModel extends ConfigDBModel

trait JdbcConfigDBModel extends ConfigDBModel

trait TelemetryConfigDBModel extends ConfigDBModel

trait NifiConfigDBModel extends ConfigDBModel

trait CompilerConfigDBModel extends ConfigDBModel


case class SolrConfigDBModelV1(zookeeperConnections: ZookeeperConnectionsConfig,
                               name: String) extends SolrConfigDBModel

case class HBaseConfigDBModelV1(coreSiteXmlPath: String,
                                hbaseSiteXmlPath: String,
                                others: Seq[HBaseEntryConfig],
                                name: String) extends HBaseConfigDBModel

case class KafkaConfigDBModelV1(connections: Seq[ConnectionConfig],
                                ingest_rate: String,
                                zookeeperConnections: ZookeeperConnectionsConfig,
                                broker_id: String,
                                partitioner_fqcn: String,
                                default_encoder: String,
                                key_encoder_fqcn: String,
                                encoder_fqcn: String,
                                decoder_fqcn: String,
                                batch_send_size: Int,
                                acks: String,
                                others: Seq[KafkaEntryConfig],
                                name: String) extends KafkaConfigDBModel

case class SparkBatchConfigDBModelV1(appName: String,
                                     master: ConnectionConfig,
                                     driver: SparkDriverConfig,
                                     executorCores: Int,
                                     executorMemory: String,
                                     coresMax: Int,
                                     executorInstances: Int,
                                     additionalJarsPath: String,
                                     yarnJar: String,
                                     blockManagerPort: Int,
                                     retained: RetainedConfigModel,
                                     kryoSerializer: KryoSerializerConfig,
                                     others: Seq[SparkEntryConfig],
                                     name: String) extends SparkBatchConfigDBModel

case class SparkStreamingConfigDBModelV1(appName: String,
                                         master: ConnectionConfig,
                                         driver: SparkDriverConfig,
                                         executorCores: Int,
                                         executorMemory: String,
                                         coresMax: Int,
                                         executorInstances: Int,
                                         additionalJarsPath: String,
                                         yarnJar: String,
                                         blockManagerPort: Int,
                                         retained: RetainedConfigModel,
                                         kryoSerializer: KryoSerializerConfig,
                                         streamingBatchIntervalMs: Int,
                                         checkpointDir: String,
                                         enableHiveSupport: Boolean,
                                         triggerIntervalMs: Option[Long],
                                         others: Seq[SparkEntryConfig],
                                         nifiStateless: Option[NifiStatelessConfigModel],
                                         schedulingStrategy: SchedulingStrategyConfigModel,
                                         name: String) extends SparkStreamingConfigDBModel

case class ElasticConfigDBModelV1(connections: Seq[ConnectionConfig],
                                  name: String
                                 ) extends ElasticConfigDBModel

case class JdbcConfigDBModelV1(connections: Map[String, JdbcConnectionConfig],
                               name: String
                              ) extends JdbcConfigDBModel

case class TelemetryConfigDBModelV1(val name: String,
                                    writer: String,
                                    sampleOneMessageEvery: Int,
                                    telemetryTopicConfigModel: TelemetryTopicConfigModel
                                   ) extends TelemetryConfigDBModel

case class NifiConfigDBModelV1(nifiBaseUrl: String,
                               nifiApiPath: String,
                               nifiUiPath: String,
                               name: String
                              ) extends NifiConfigDBModel

case class CompilerConfigDBModelV1(compilerInstances: Int,
                                   name: String
                                  ) extends CompilerConfigDBModel


//object HBaseConfigDBModel{
//  val TYPE = "HBase"
//}
// TESTME: not needed?
//object SolrConfigDBModel{
//  val TYPE = "Solr"
//}

