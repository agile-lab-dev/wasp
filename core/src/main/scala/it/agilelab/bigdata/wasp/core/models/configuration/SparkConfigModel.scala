package it.agilelab.bigdata.wasp.core.models.configuration

import it.agilelab.bigdata.wasp.core.models.Model
import it.agilelab.bigdata.wasp.core.utils.{ConnectionConfig, SparkDriverConfig}

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
	val broadcastPort: Int
	val fileserverPort: Int
	val retainedStagesJobs: Int
	val retainedJobs: Int
	val retainedTasks: Int
	val retainedExecutions: Int
	val retainedBatches: Int
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
																			broadcastPort: Int,
																			fileserverPort: Int,
																			retainedStagesJobs: Int,
																			retainedTasks: Int,
																			retainedJobs: Int,
																			retainedExecutions: Int,
																			retainedBatches: Int,

																			streamingBatchIntervalMs: Int,
																			checkpointDir: String,

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
																	broadcastPort: Int,
																	fileserverPort: Int,
																	retainedStagesJobs: Int,
																	retainedTasks: Int,
																	retainedJobs: Int,
																	retainedExecutions: Int,
																	retainedBatches: Int,

																	name: String
																) extends SparkConfigModel