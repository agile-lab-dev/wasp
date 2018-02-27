package it.agilelab.bigdata.wasp.consumers.spark.utils

import java.io.File
import java.net.URLEncoder

import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.models.configuration.{SparkConfigModel, SparkStreamingConfigModel}
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, ElasticConfiguration, WaspConfiguration}
import org.apache.spark.SparkConf

import scala.io.Source

/**
	* Utilities related to Spark.
	*
	* @author Nicolò Bidotti
	*/
object SparkUtils extends Logging with WaspConfiguration with ElasticConfiguration {
  val jarsListFileName = "jars.list"

  /**
    * Builds a SparkConf from the supplied SparkConfigModel
    */
  def buildSparkConfFromSparkConfigModel(sparkConfigModel: SparkConfigModel): SparkConf = {
    logger.info("Building Spark configuration from configuration model")
    
    // validate SparkConfigModel & log it
    validateConfig(sparkConfigModel)
    logger.info(s"Starting from SparkConfigModel:\n\t$sparkConfigModel")
  
    // build SparkConf from SparkConfigModel & log it
    var sparkConf = new SparkConf()
      .setAppName(sparkConfigModel.appName)
      .setMaster(sparkConfigModel.master.toString)
      .set("spark.submit.deployMode", sparkConfigModel.driver.submitDeployMode)  // where the driver have to be executed (client or cluster)
      .set("spark.driver.cores", sparkConfigModel.driver.cores.toString)
      .set("spark.driver.memory", sparkConfigModel.driver.memory) // NOTE: will only work in yarn-cluster
      .set("spark.driver.host", sparkConfigModel.driver.host)
      .set("spark.driver.bindAddress", sparkConfigModel.driver.bindAddress)
    if (sparkConfigModel.driver.port != 0)
      sparkConf = sparkConf.set("spark.driver.port", sparkConfigModel.driver.port.toString)

    sparkConf
      .set("spark.executor.cores", sparkConfigModel.executorCores.toString)
      .set("spark.executor.memory", sparkConfigModel.executorMemory)
      .set("spark.executor.instances", sparkConfigModel.executorInstances.toString)
      .setJars(getAdditionalJars(sparkConfigModel.additionalJarsPath))
      .set("spark.yarn.jars", sparkConfigModel.yarnJar)
      .set("spark.blockManager.port", sparkConfigModel.blockManagerPort.toString)
      .set("spark.broadcast.port", sparkConfigModel.broadcastPort.toString)
      .set("spark.fileserver.port", sparkConfigModel.fileserverPort.toString)
      .set("spark.ui.retainedStages", sparkConfigModel.retainedStagesJobs.toString)
      .set("spark.ui.retainedTasks", sparkConfigModel.retainedTasks.toString)
      .set("spark.ui.retainedJobs", sparkConfigModel.retainedStagesJobs.toString)
      .set("spark.sql.ui.retainedExecutions", sparkConfigModel.retainedExecutions.toString)
      .set("spark.streaming.ui.retainedBatches", sparkConfigModel.retainedBatches.toString)

    // add specific Elastic configurations
    val conns = elasticConfig.connections.filter(_.metadata.flatMap(_.get("connectiontype")).getOrElse("") == "rest")
    val address = conns.map(e => s"${e.host}:${e.port}").mkString(",")
    sparkConf = sparkConf.set("es.nodes", address)


    logger.info(s"Resulting SparkConf:\n\t${sparkConf.toDebugString.replace("\n", "\n\t")}")

    sparkConf
  }

  private def getAdditionalJars(additionalJarsPath: String): Seq[String] = {
    try {
      val additionalJars = Source.fromFile(additionalJarsPath + File.separator + jarsListFileName)
        .getLines()
        .map(name => URLEncoder.encode(additionalJarsPath  + File.separator + name, "UTF-8"))
        .toSeq

      additionalJars
    } catch {
      case e : Throwable =>
        val msg = s"Unable to completely generate the additional jars list - Exception: ${e.getMessage}"
        logger.error(msg, e)
        throw e
    }
  }
  
  def validateConfig(sparkConfig: SparkConfigModel): Unit = {
    val master = sparkConfig.master
    if (master.protocol == "" && master.host.startsWith("yarn"))
      logger.warn("Running on YARN without specifying spark.yarn.jar is unlikely to work!")
  }
	
	def generateLegacyStreamingCheckpointDir(sparkStreamingConfigModel: SparkStreamingConfigModel): String = {

    val environmentPrefix = ConfigManager.getWaspConfig.environmentPrefix

    val prefix = if (environmentPrefix == "") "" else "/"+environmentPrefix

		sparkStreamingConfigModel.checkpointDir + prefix + "/" +
			"legacy_streaming"
	}
	
  def generateStructuredStreamingCheckpointDir(sparkStreamingConfigModel: SparkStreamingConfigModel,
                                               pipegraph: PipegraphModel,
                                               component: StructuredStreamingETLModel): String = {

    val environmentPrefix = ConfigManager.getWaspConfig.environmentPrefix

    val prefix = if (environmentPrefix == "") "" else "/"+environmentPrefix

    sparkStreamingConfigModel.checkpointDir + prefix + "/" +
	    "structured_streaming" + "/" +
	    pipegraph.generateStandardPipegraphName + "/" +
	    component.generateStandardProcessingComponentName + "_" + component.generateStandardWriterName
  }
}