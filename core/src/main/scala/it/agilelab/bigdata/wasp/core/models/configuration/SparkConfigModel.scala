package it.agilelab.bigdata.wasp.core.models.configuration

import it.agilelab.bigdata.wasp.core.utils.ConnectionConfig


trait SparkConfigModel {
	def appName: String
	def master: ConnectionConfig
	def driverCores: Int
	def driverMemory: String
	def driverHostname: String
	def driverPort: Int
	def executorCores: Int
	def executorMemory: String
	def executorInstances: Int
	def additionalJars: Option[Seq[String]]
	def yarnJar: String
	def blockManagerPort: Int
	def broadcastPort: Int
	def fileserverPort: Int
	def name: String
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
                                     additionalJars: Option[Seq[String]],
                                     yarnJar: String,
                                     blockManagerPort: Int,
                                     broadcastPort: Int,
                                     fileserverPort: Int,
                                     streamingBatchIntervalMs: Int,
                                     checkpointDir: String,
                                     name: String) extends SparkConfigModel

case class SparkBatchConfigModel(appName: String,
                                 master: ConnectionConfig,
                                 driverCores: Int,
                                 driverMemory: String,
                                 driverHostname: String,
                                 driverPort: Int,
                                 executorCores: Int,
                                 executorMemory: String,
                                 executorInstances: Int,
                                 additionalJars: Option[Seq[String]],
                                 yarnJar: String,
                                 blockManagerPort: Int,
                                 broadcastPort: Int,
                                 fileserverPort: Int,
                                 name: String)  extends SparkConfigModel
