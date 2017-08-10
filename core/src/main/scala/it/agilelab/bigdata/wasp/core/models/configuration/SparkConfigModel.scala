package it.agilelab.bigdata.wasp.core.models.configuration

import it.agilelab.bigdata.wasp.core.utils.ConnectionConfig

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
                                     name: String)

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
                                 name: String)
