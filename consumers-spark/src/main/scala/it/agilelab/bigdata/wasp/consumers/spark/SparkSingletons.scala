package it.agilelab.bigdata.wasp.consumers.spark

import java.io.File

import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.SparkConfigModel
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.apache.spark.{SparkConf, SparkContext}


object SparkSingletons extends Logging {
  
  private var sparkContext: SparkContext = _
  
  /**
    * Try to initialize the SparkContext in the SparkSingleton with the provided configuration.
    *
    * If the SparkContext does not exist, it will be created using the settings from sparkConfig,
    * and true will be returned.
    * If the SparkContext already exists, nothing will be done, and false will be returned.
    */
  def createSparkContext(sparkConfig: SparkConfigModel): Boolean =
    SparkSingletons.synchronized {
      if (sparkContext == null) {
        // validate config & log it
        validateConfig(sparkConfig)
        logger.info("Spark config model:\n" + sparkConfig)
        
        // build SparkConf from spark configuration model & log it
        val loadedJars = sparkConfig.additionalJars.getOrElse(getAdditionalJar(Set(sparkConfig.yarnJar)))
        //TODO: gestire appName in maniera dinamica (e.g. batchGuardian, consumerGuardian..)
        val conf = new SparkConf()
          .setAppName(sparkConfig.appName)
          .setMaster(sparkConfig.master.toString)
          .set("spark.driver.cores", sparkConfig.driverCores.toString)
          .set("spark.driver.memory", sparkConfig.driverMemory) // NOTE: will only work in yarn-cluster
          .set("spark.driver.host", sparkConfig.driverHostname)
          .set("spark.driver.port", sparkConfig.driverPort.toString)
          .set("spark.executor.cores", sparkConfig.executorCores.toString)
          .set("spark.executor.memory", sparkConfig.executorMemory)
          .set("spark.executor.instances", sparkConfig.executorInstances.toString)
          .setJars(loadedJars)
          .set("spark.yarn.jar", sparkConfig.yarnJar)
          .set("spark.blockManager.port", sparkConfig.blockManagerPort.toString)
          .set("spark.broadcast.port", sparkConfig.broadcastPort.toString)
          .set("spark.fileserver.port", sparkConfig.fileserverPort.toString)
        logger.info("SparkConf:\n\t" + conf.toDebugString.replace("\n", "\n\t"))
        
        // instantiate SparkContext
        logger.info("Instantiating SparkContext...")
        sparkContext = new SparkContext(conf)
        logger.info("Successfully instantiated SparkContext.")
        
        true
      } else {
        false
      }
    }

  /**
    * Returns the SparkContext held by this SparkHolder, or throws an exception if it was not initialized.
    */
  def getSparkContext: SparkContext =
    SparkSingletons.synchronized {
      if (sparkContext == null) {
        throw new Exception("The SparkContext was not initialized")
      }
      sparkContext
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

}
