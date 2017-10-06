package it.agilelab.bigdata.wasp.consumers.spark

import java.io.File
import java.lang

import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.{SparkConfigModel, SparkStreamingConfigModel}
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Singletons an initialization code related to Spark.
  *
  * While Spark provides various getOrCreate methods, this allows WASP to initialize Spark in a cleaner way and in
  * just a few places and access the various entry points in a much more straightforward way.
  */
object SparkSingletons extends Logging {
  
  private var sparkSession: SparkSession = _
  private var sparkContext: SparkContext = _
  private var sqlContext: SQLContext = _
  private var streamingContext: StreamingContext = _
  
  /**
    * Try to initialize the SparkSession in the SparkSingleton with the provided configuration.
    *
    * If it does not exist, it will be created using the settings from
    * `sparkConfigModel` and true will be returned.
    *
    * If the SparkSession already exists, nothing will be done, and false will be returned.
    *
    * @throws IllegalStateException if Spark was already initialized but <b>not by using this method</b>
    */
  // TODO we can detect that a SparkSession was already created; how to detect that a SparkContext was already created?
  @throws[lang.IllegalStateException]
  def initializeSpark(sparkConfigModel: SparkConfigModel): Boolean =
    SparkSingletons.synchronized {
      if (sparkSession == null) { // we don't have a singleton ready
        // TODO fix race condition: SparkSession being created after this check is false but before we do it in the else branch
        if (SparkSession.getDefaultSession.isDefined) { // global default session found
          // there's a valid global default session that we did not create!
          // throw an exception as this should never happen
          // if we do not control the creation we are not sure that a sensible config was used
          // TODO demote to warning?
          throw new IllegalStateException("Spark was already intialized without using this method!")
        } else { // no global default session
          val sparkConf = buildSparkConfFromSparkConfigModel(sparkConfigModel)
  
          // instantiate & assign SparkSession
          logger.info("Instantiating SparkSession...")
          sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
          logger.info("Successfully instantiated SparkSession.")
          
          // assign SparkContext & SQLContext
          sparkContext = sparkSession.sparkContext
          sqlContext = sparkSession.sqlContext
  
          true
        }
      } else {
        false
      }
    }
  
  /**
    * Try to initialize the StreamingContext in the SparkSingleton with the provided configuration.
    *
    * This must be called after initializeSpark is called with the same configuration to ensure that Spark is initialized.
    *
    * If it does not exist, it will be created using the settings from `sparkStreamingConfigModel` and true will be
    * returned.
    *
    * If the StreamingContext already exists, nothing will be done, and false will be returned.
    *
    * @throws IllegalStateException if Spark was not already initialized
    */
  // TODO we can detect that a SparkSession was already created; how to detect that s SparkContext was already created??
  @throws[lang.IllegalStateException]
  def initializeSparkStreaming(sparkStreamingConfigModel: SparkStreamingConfigModel): Boolean =
    SparkSingletons.synchronized {
      if (streamingContext == null) { // we don't have a singleton ready
        if (sparkSession == null) {
          throw new IllegalStateException("Spark was not initialized; invoke initializeSpark with the same configuration" +
                                          "before calling initializeSparkStreaming")
        }
        
        // instantiate & assign StreamingContext
        val batchDuration = Milliseconds(sparkStreamingConfigModel.streamingBatchIntervalMs)
        streamingContext = new StreamingContext(getSparkContext, batchDuration)
  
        true
      } else {
        false
      }
    }
  
  /**
    * Returns the SparkSession singleton, or throws an exception if Spark was not initialized.
    *
    * @throws IllegalStateException if Spark was not already initialized
    */
  @throws[lang.IllegalStateException]
  def getSparkSession: SparkSession =
    SparkSingletons.synchronized {
      if (sparkSession == null) {
        throw new IllegalStateException("Spark was not initialized; invoke initializeSpark with a proper configuration" +
                                        "before calling this getter")
      }
      sparkSession
    }

  /**
    * Returns the SparkContext singleton, or throws an exception if Spark was not initialized.
    *
    * @throws IllegalStateException if Spark was not already initialized
    */
  @throws[lang.IllegalStateException]
  def getSparkContext: SparkContext =
    SparkSingletons.synchronized {
      if (sparkContext == null) {
        throw new IllegalStateException("Spark was not initialized; invoke initializeSpark with a proper configuration" +
                                        "before calling this getter")
      }
      sparkContext
    }
  
  /**
    * Returns the SQLContext singleton, or throws an exception if Spark was not initialized.
    *
    * @throws IllegalStateException if Spark was not already initialized
    */
  @throws[lang.IllegalStateException]
  def getSQLContext: SQLContext =
    SparkSingletons.synchronized {
      if (sqlContext == null) {
        throw new IllegalStateException("Spark was not initialized; invoke initializeSpark with a proper configuration" +
                                        "before calling this getter")
      }
      sqlContext
    }
  
  /**
    * Returns the StreamingContext singleton, or throws an exception if Spark Streaming was not initialized.
    *
    * @throws IllegalStateException if Spark was not already initialized
    */
  @throws[lang.IllegalStateException]
  def getStreamingContext: StreamingContext =
    SparkSingletons.synchronized {
      if (streamingContext == null) {
        throw new IllegalStateException("Spark Streaming was not initialized; invoke initializeSparkStreaming with a proper" +
                                        "configuration before calling this getter")
      }
      streamingContext
    }
  
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

}
