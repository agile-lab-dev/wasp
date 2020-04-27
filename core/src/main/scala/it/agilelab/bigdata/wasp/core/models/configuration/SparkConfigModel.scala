package it.agilelab.bigdata.wasp.core.models.configuration

import it.agilelab.bigdata.wasp.core.models.Model
import it.agilelab.bigdata.wasp.core.utils.ConnectionConfig

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
	val retainedStagesJobs: Int
	val retainedJobs: Int
	val retainedTasks: Int
	val retainedExecutions: Int
	val retainedBatches: Int
  val kryoSerializer: KryoSerializerConfig
  val others: Seq[SparkEntryConfig]
}

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
																			retainedStagesJobs: Int,
																			retainedTasks: Int,
																			retainedJobs: Int,
																			retainedExecutions: Int,
																			retainedBatches: Int,
																			kryoSerializer: KryoSerializerConfig,
																			streamingBatchIntervalMs: Int,
																			checkpointDir: String,
																			triggerIntervalMs: Option[Long],
																			others: Seq[SparkEntryConfig],
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
																	retainedStagesJobs: Int,
																	retainedTasks: Int,
																	retainedJobs: Int,
																	retainedExecutions: Int,
																	retainedBatches: Int,
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