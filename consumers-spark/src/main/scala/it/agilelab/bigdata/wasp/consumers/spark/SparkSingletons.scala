package it.agilelab.bigdata.wasp.consumers.spark

import java.lang

import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.{SparkConfigModel, SparkStreamingConfigModel}
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkUtils.{logger, _}
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.apache.hadoop.fs.Path
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkException}

import collection.JavaConverters._


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
          throw new IllegalStateException("Spark was already initialized without using this method!")
        } else { // no global default session
          logger.info("Initializing Spark...")
          
          val sparkConf = buildSparkConfFromSparkConfigModel(sparkConfigModel)
          
          // try and build SparkContext even if the SparkSession would do it anyway
          // to ensure we are the one initializing Spark
          try {
            logger.info("Instantiating SparkContext...")
            new SparkContext(sparkConf)
            logger.info("Successfully instantiated SparkContext")
          } catch {
            case se: SparkException if se.getMessage.contains("SPARK-2243") =>
              // another SparkContext was already running; this means we did not create it!
              // the guard with the .contains("SPARK-2243") ensures we only catch the SparkException thrown by Spark in
              // SparkContext.assertNoOtherContextIsRunning; SPARK-2243 is a JIRA for multiple SparkContext in the same JVM
              // throw an exception as this should never happen
              // if we do not control the creation we are not sure that a sensible config was used
              // TODO demote to warning?
              throw new IllegalStateException("Spark was already initialized without using this method!")
          }
  
          // instantiate & assign SparkSession
          logger.info("Instantiating SparkSession...")
          sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
          logger.info("Successfully instantiated SparkSession")
          logger.info(s"SparkContext configuration: ${sparkSession.sparkContext.hadoopConfiguration.toString}")
          logger.info(s"SparkHadoopUtil configuration: ${SparkHadoopUtil.get.conf.toString}")

          // assign SparkContext & SQLContext
          sparkContext = sparkSession.sparkContext
          sqlContext = sparkSession.sqlContext
  
          logger.info("Successfully initialized Spark")
  
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
  @throws[lang.IllegalStateException]
  def initializeSparkStreaming(sparkStreamingConfigModel: SparkStreamingConfigModel): Boolean =
    SparkSingletons.synchronized {
      if (streamingContext == null) { // we don't have a singleton ready
        if (sparkSession == null) {
          throw new IllegalStateException("Spark was not initialized; invoke initializeSpark with the same configuration " +
                                          "before calling initializeSparkStreaming")
        }
        
        // validate config & log it
        validateConfig(sparkStreamingConfigModel)
        logger.info("Using SparkStreamingConfigModel:\n" + sparkStreamingConfigModel)
        
        // instantiate & assign StreamingContext
        logger.info("Instantiating StreamingContext...")
        val legacyStreamingCheckpointDir = generateLegacyStreamingCheckpointDir(sparkStreamingConfigModel)
        def createStreamingContext: () => StreamingContext = () => { // helper to create StreamingContext
          val batchDuration = Milliseconds(sparkStreamingConfigModel.streamingBatchIntervalMs)
          val newStreamingContext = new StreamingContext(getSparkContext, batchDuration)
          newStreamingContext.checkpoint(legacyStreamingCheckpointDir)
          newStreamingContext
        }
        streamingContext = StreamingContext.getOrCreate(legacyStreamingCheckpointDir, createStreamingContext)
        logger.info("Successfully instantiated StreamingContext")
  
        true
      } else {
        false
      }
    }
  
  /**
    * Try to deinitialize the StreamingContext in the SparkSingleton.
    *
    * This only removes the reference to the StreamingContext in the SparkSingletons; <b>it does not stop it.</b>
    */
  @throws[lang.IllegalStateException]
  def deinitializeSparkStreaming(): Unit =
    SparkSingletons.synchronized {
      streamingContext = null
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
        throw new IllegalStateException("Spark was not initialized; invoke initializeSpark with a proper configuration " +
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
        throw new IllegalStateException("Spark was not initialized; invoke initializeSpark with a proper configuration " +
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
        throw new IllegalStateException("Spark was not initialized; invoke initializeSpark with a proper configuration " +
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
        throw new IllegalStateException("Spark Streaming was not initialized; invoke initializeSparkStreaming with a proper " +
                                        "configuration before calling this getter")
      }
      streamingContext
    }
}
