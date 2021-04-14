package it.agilelab.bigdata.wasp.models.configuration

import com.typesafe.config.Config
import it.agilelab.bigdata.wasp.models.Model

trait SparkConfigModel extends Model {
  val appName: String
  val master: ConnectionConfig
  val driver: SparkDriverConfig
  val executorCores: Int
  val executorMemory: String
  val coresMax: Int
  val executorInstances: Int
  val additionalJarsPath: String
  val yarnJar: String
  val blockManagerPort: Int
  val retained: RetainedConfigModel
  val kryoSerializer: KryoSerializerConfig
  val others: Seq[SparkEntryConfig]
}

case class NifiStatelessConfigModel(
                                     bootstrapJars: String,
                                     systemJars: String,
                                     statelessJars: String,
                                     extensions: String)

case class RetainedConfigModel(retainedStagesJobs: Int,
                               retainedTasks: Int,
                               retainedJobs: Int,
                               retainedExecutions: Int,
                               retainedBatches: Int)

case class SchedulingStrategyConfigModel(
                                          factoryClass: String,
                                          factoryParams: Config
                                        )

case class SparkStreamingConfigModel(
                                      appName: String,
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
                                      name: String
                                    ) extends SparkConfigModel

case class SparkBatchConfigModel(
                                  appName: String,
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
                                  name: String
                                ) extends SparkConfigModel

case class SparkDriverConfig(
                              submitDeployMode: String,
                              cores: Int,
                              memory: String,
                              host: String,
                              bindAddress: String,
                              port: Int,
                              killDriverProcessIfSparkContextStops: Boolean
                            )

case class KryoSerializerConfig(
                                 enabled: Boolean,
                                 registrators: String,
                                 strict: Boolean
                               )

case class SparkEntryConfig(
                             key: String,
                             value: String
                           )