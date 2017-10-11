package it.agilelab.bigdata.wasp.consumers.spark.utils

import java.io.File

import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.models.configuration.{SparkConfigModel, SparkStreamingConfigModel}
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.apache.spark.SparkConf

/**
	* Utilities related to Spark.
	*
	* @author Nicolò Bidotti
	*/
object SparkUtils extends Logging {
	/**
    * Builds a SparkConf from the supplied SparkConfigModel
    */
  def buildSparkConfFromSparkConfigModel(sparkConfigModel: SparkConfigModel): SparkConf = {
    logger.info("Building Spark configuration from configuration model")
    
    // validate config & log it
    validateConfig(sparkConfigModel)
    logger.info("Starting from SparkConfigModel:\n" + sparkConfigModel)
  
    // build SparkConf from spark configuration model & log it
    val loadedJars = sparkConfigModel.additionalJars.getOrElse(getAdditionalJar(Set(sparkConfigModel.yarnJar)))
    //TODO: gestire appName in maniera dinamica (e.g. batchGuardian, consumerGuardian..)
    val sparkConf = new SparkConf()
      .setAppName(sparkConfigModel.appName)
      .setMaster(sparkConfigModel.master.toString)
      .set("spark.driver.cores", sparkConfigModel.driverCores.toString)
      .set("spark.driver.memory", sparkConfigModel.driverMemory) // NOTE: will only work in yarn-cluster
      .set("spark.driver.host", sparkConfigModel.driverHostname)
      .set("spark.driver.port", sparkConfigModel.driverPort.toString)
      .set("spark.executor.cores", sparkConfigModel.executorCores.toString)
      .set("spark.executor.memory", sparkConfigModel.executorMemory)
      .set("spark.executor.instances", sparkConfigModel.executorInstances.toString)
      .setJars(loadedJars)
      .set("spark.yarn.jar", sparkConfigModel.yarnJar)
      .set("spark.blockManager.port", sparkConfigModel.blockManagerPort.toString)
      .set("spark.broadcast.port", sparkConfigModel.broadcastPort.toString)
      .set("spark.fileserver.port", sparkConfigModel.fileserverPort.toString)
    logger.info("Resulting SparkConf:\n\t" + sparkConf.toDebugString.replace("\n", "\n\t"))
    
    sparkConf
  }
  
  def getAdditionalJar(skipJars: Set[String]): Seq[String] = {

    val additionalJarsPath = ConfigManager.getWaspConfig.additionalJarsPath

    val fm = getListOfFiles(s"${additionalJarsPath}/managed/")
    val f = getListOfFiles(s"${additionalJarsPath}/")

    val allFilesSet = fm
        .map(f => s"${additionalJarsPath}/managed/${f}")
        .toSet[String] ++ f.map(f => s"${additionalJarsPath}/${f}").toSet[String]

    val realFiles = allFilesSet.diff(skipJars)

    realFiles.toSeq
  }

  def getListOfFiles(dir: String): List[String] = {

    val d = new File(dir)

    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).map(_.getName).toList
    } else {
      List[String]()
    }
  }
  
  def validateConfig(sparkConfig: SparkConfigModel): Unit = {
    val master = sparkConfig.master
    if (master.protocol == "" && master.host.startsWith("yarn"))
      logger.warn("Running on YARN without specifying spark.yarn.jar is unlikely to work!")
  }
	
	def generateLegacyStreamingCheckpointDir(sparkStreamingConfigModel: SparkStreamingConfigModel): String = {
		sparkStreamingConfigModel.checkpointDir + "/" +
			"legacy_streaming"
	}
	
  def generateStructuredStreamingCheckpointDir(sparkStreamingConfigModel: SparkStreamingConfigModel,
                                               pipegraph: PipegraphModel,
                                               component: StructuredStreamingETLModel): String = {
    sparkStreamingConfigModel.checkpointDir + "/" +
	    "structured_streaming" + "/" +
	    generatePipegraphPart(pipegraph) + "/" +
	    generateProcessingComponentPart(component) + "_" + generateWriterPart(component)
  }
	
	def generateUniqueComponentName(pipegraph: PipegraphModel,
	                                component: ProcessingComponentModel): String = {
		generatePipegraphPart(pipegraph) + "_" + generateProcessingComponentPart(component) + "_" + generateWriterPart(component)
	}
	
	private def generatePipegraphPart(pipegraph: PipegraphModel): String = {
		s"pipegraph_${pipegraph.name}"
	}
	
	private def generateProcessingComponentPart(component: ProcessingComponentModel): String = {
		component match { // TODO: remove match once name member is moved into ProcessingComponentModel
			case ss: StructuredStreamingETLModel => s"structuredstreaming_${ss.name}"
			case ls: LegacyStreamingETLModel => s"legacystreaming_${ls.name}"
			case rt: RTModel => s"rt_${rt.name}"
		}
	}
	
	private def generateWriterPart(component: ProcessingComponentModel): String = {
		component match { // TODO: remove match once output member is moved into ProcessingComponentModel
			case ss: StructuredStreamingETLModel => s"writer_${ss.output.name}"
			case ls: LegacyStreamingETLModel => s"writer_${ls.output.name}"
			case rt: RTModel => rt.endpoint match {
				case Some(output) => s"writer_${output.name}"
				case None => "no_writer"
			}
		}
	}
}
