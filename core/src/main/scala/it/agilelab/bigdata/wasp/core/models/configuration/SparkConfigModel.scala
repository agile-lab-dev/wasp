package it.agilelab.bigdata.wasp.core.models.configuration

import it.agilelab.bigdata.wasp.core.models.Model
import it.agilelab.bigdata.wasp.core.utils.{ConnectionConfig, KryoSerializerConfig, SparkDriverConfig}

trait SparkConfigModel extends Model {
	val appName: String
	val master: ConnectionConfig
	val driver: SparkDriverConfig
	val executorCores: Int
	val executorMemory: String
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
  val others: Seq[SparkEntryConfigModel]
}

case class SparkStreamingConfigModel(
																			appName: String,
																			master: ConnectionConfig,
																			driver: SparkDriverConfig,
																			executorCores: Int,
																			executorMemory: String,
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
                                      others: Seq[SparkEntryConfigModel],

																			name: String
																		) extends SparkConfigModel

case class SparkBatchConfigModel(
																	appName: String,
																	master: ConnectionConfig,
																	driver: SparkDriverConfig,
																	executorCores: Int,
																	executorMemory: String,
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
                                  others: Seq[SparkEntryConfigModel],
																	name: String
																) extends SparkConfigModel

case class SparkEntryConfigModel(
																key: String,
																value: String
																)