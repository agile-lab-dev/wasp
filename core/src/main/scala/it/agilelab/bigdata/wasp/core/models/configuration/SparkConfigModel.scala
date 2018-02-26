package it.agilelab.bigdata.wasp.core.models.configuration

import it.agilelab.bigdata.wasp.core.models.Model
import it.agilelab.bigdata.wasp.core.utils.ConnectionConfig


trait SparkConfigModel extends Model {
	val appName: String
	val master: ConnectionConfig
	val driverCores: Int
	val driverMemory: String
	val driverHostname: String
	val driverPort: Int
	val executorCores: Int
	val executorMemory: String
	val executorInstances: Int
	val additionalJarsPath: String
	val yarnJar: String
	val blockManagerPort: Int
	val broadcastPort: Int
	val fileserverPort: Int
	val driverBindAddress: String
	val retainedStagesJobs: Int
	val retainedTasks: Int
	val retainedExecutions: Int
	val retainedBatches: Int
}

case class SparkStreamingConfigModel(appName: String,
                                     master: ConnectionConfig,
                                     driverCores: Int,
                                     driverMemory: String,
                                     driverHostname: String,
                                     driverPort: Int,
                                     executorCores: Int,
                                     executorMemory: String,
                                     executorInstances: Int,
																		 additionalJarsPath: String,
                                     yarnJar: String,
                                     blockManagerPort: Int,
                                     broadcastPort: Int,
                                     fileserverPort: Int,
                                     streamingBatchIntervalMs: Int,
                                     checkpointDir: String,
                                     name: String,
																		 driverBindAddress: String,
																		 retainedStagesJobs:Int,
																		 retainedTasks: Int,
																		 retainedExecutions: Int,
																		 retainedBatches: Int) extends SparkConfigModel

case class SparkBatchConfigModel(appName: String,
                                 master: ConnectionConfig,
                                 driverCores: Int,
                                 driverMemory: String,
                                 driverHostname: String,
                                 driverPort: Int,
                                 executorCores: Int,
                                 executorMemory: String,
                                 executorInstances: Int,
																 additionalJarsPath: String,
                                 yarnJar: String,
                                 blockManagerPort: Int,
                                 broadcastPort: Int,
                                 fileserverPort: Int,
                                 name: String,
																 driverBindAddress: String,
																 retainedStagesJobs:Int,
																 retainedTasks: Int,
																 retainedExecutions: Int,
																 retainedBatches: Int) extends SparkConfigModel
