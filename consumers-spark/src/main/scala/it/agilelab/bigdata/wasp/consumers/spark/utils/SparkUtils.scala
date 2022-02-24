package it.agilelab.bigdata.wasp.consumers.spark.utils

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.Base64

import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.configuration.{KafkaConfigModel, NifiStatelessConfigModel, SparkConfigModel, SparkStreamingConfigModel, TelemetryConfigModel}
import it.agilelab.bigdata.wasp.core.utils.{ElasticConfiguration, SparkStreamingConfiguration, WaspConfiguration}
import it.agilelab.bigdata.wasp.models.{PipegraphModel, StructuredStreamingETLModel}
import org.apache.spark.{SparkConf, UtilsForwarder}

import scala.io.Source

/**
	* Utilities related to Spark.
	*
	* @author Nicolò Bidotti
	*/
object SparkUtils extends Logging with WaspConfiguration with ElasticConfiguration with SparkStreamingConfiguration {
  val jarsListFileName = "jars.list"

  /**
    * Builds a SparkConf from the supplied SparkConfigModel
    */
  def buildSparkConfFromSparkConfigModel(
      sparkConfigModel: SparkConfigModel,
      telemetryConfig: TelemetryConfigModel,
      kafkaConfigModel: KafkaConfigModel
  ): SparkConf = {
    logger.info("Building Spark configuration from configuration model")

    logger.info(s"Starting from SparkConfigModel:\n\t$sparkConfigModel")

    // build SparkConf from SparkConfigModel & log it
    val sparkConf = new SparkConf()
      .setAppName(sparkConfigModel.appName)
      .setMaster(sparkConfigModel.master.toString)

    // driver-related configs
    sparkConf
      .set("spark.submit.deployMode", sparkConfigModel.driver.submitDeployMode) // where the driver have to be executed (client or cluster)
      .set("spark.driver.cores", sparkConfigModel.driver.cores.toString)
      .set("spark.driver.memory", sparkConfigModel.driver.memory) // NOTE: will only work in yarn-cluster
      .set("spark.driver.host", sparkConfigModel.driver.host)
      .set("spark.driver.bindAddress", sparkConfigModel.driver.bindAddress)
    if (sparkConfigModel.driver.port != 0)
      sparkConf.set("spark.driver.port", sparkConfigModel.driver.port.toString)

    sparkConf
      .set("spark.executor.cores", sparkConfigModel.executorCores.toString)
      .set("spark.executor.memory", sparkConfigModel.executorMemory)
      .set("spark.cores.max", sparkConfigModel.coresMax.toString)
      .set("spark.executor.instances", sparkConfigModel.executorInstances.toString)
      .setJars(getAdditionalJars(sparkConfigModel.additionalJarsPath))
      .set("spark.yarn.jars", sparkConfigModel.yarnJar)
      .set("spark.blockManager.port", sparkConfigModel.blockManagerPort.toString)
      .set("spark.ui.retainedStages", sparkConfigModel.retained.retainedStagesJobs.toString)
      .set("spark.ui.retainedTasks", sparkConfigModel.retained.retainedTasks.toString)
      .set("spark.ui.retainedJobs", sparkConfigModel.retained.retainedJobs.toString)
      .set("spark.sql.ui.retainedExecutions", sparkConfigModel.retained.retainedExecutions.toString)
      .set("spark.streaming.ui.retainedBatches", sparkConfigModel.retained.retainedBatches.toString)
      .setAll(sparkConfigModel.others.map(v => (v.key, v.value)))

    // kryo-related configs
    if (sparkConfigModel.kryoSerializer.enabled) {
      // This setting configures the serializer used for not only shuffling data between worker nodes but also when serializing RDDs to disk.
      // N.B. The only reason Kryo is not the default is because of the custom registration requirement
      sparkConf
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.set("spark.kryoserializer.buffer", "64k")  // default: 64k
      //.set("spark.kryoserializer.buffer.max", "64m") // default: 64m

      /* Registering classes: for best performance */
      // * wasp-internal class registrations
      sparkConf.registerKryoClasses(
        Array(
//          classOf[Array[Array[Byte]]],
//          classOf[scala.collection.mutable.WrappedArray.ofRef[_]], // see https://stackoverflow.com/questions/34736587/kryo-serializer-causing-exception-on-underlying-scala-class-wrappedarray
//          classOf[Array[org.apache.spark.sql.Row]] // required by 'metadata' usage of LegacyStreamingETLActor/StructuredStreamingETLActor
        )
      )
      // * standalone application class registrations (through custom-KryoRegistrators)
      sparkConf.set("spark.kryo.registrator", sparkConfigModel.kryoSerializer.registrators)

      /* strict-mode*/
      // If set to false (the default), Kryo will write unregistered class names along with each object.
      // Writing class names can cause significant performance overhead, so enabling this option can enforce strictly
      // that a user has not omitted classes from registration (i.e. will throw errors with unregistered classes).
      sparkConf.set("spark.kryo.registrationRequired", sparkConfigModel.kryoSerializer.strict.toString)

      sparkConf.set("spark.kryo.registrationRequired", sparkConfigModel.kryoSerializer.strict.toString)

      // Only for Debug - NotSerializableException: NOT REALLY USEFUL (also adding it to WASP_OPT in start-wasp.sh)
      //sparkConf.set("spark.executor.extraJavaOptions","-Dsun.io.serialization.extendedDebugInfo=true")
      //sparkConf.set("spark.driver.extraJavaOptions","-Dsun.io.serialization.extendedDebugInfo=true")
    }

    // add specific Elastic configs
    val conns   = elasticConfig.connections.filter(_.metadata.flatMap(_.get("connectiontype")).getOrElse("") == "rest")
    val address = conns.map(e => s"${e.host}:${e.port}").mkString(",")
    sparkConf.set("es.nodes", address)

    val originalExtraJavaOptions = sparkConf.get("spark.executor.extraJavaOptions", "")

    import it.agilelab.bigdata.wasp.models.configuration.TelemetryTopicConfigModelMessageFormat._

    val telemetryConfigJSON = Base64.getUrlEncoder.encodeToString(
      telemetryTopicConfigModelFormat
        .write(telemetryConfig.telemetryTopicConfigModel)
        .toString()
        .getBytes(StandardCharsets.UTF_8)
    )
    val kafkaTinyConfigJSON = Base64.getUrlEncoder.encodeToString(
      tinyKafkaConfigFormat.write(kafkaConfigModel.toTinyConfig()).toString().getBytes(StandardCharsets.UTF_8)
    )

    val newExtraJavaOptions =
      s"""$originalExtraJavaOptions -Dwasp.plugin.telemetry.kafka="$kafkaTinyConfigJSON" -Dwasp.plugin.telemetry.topic="$telemetryConfigJSON""""

    sparkConf.set("spark.executor.extraJavaOptions", newExtraJavaOptions)

    sparkConfigModel match {
      case model: SparkStreamingConfigModel =>
        model.nifiStateless.foreach {
          case NifiStatelessConfigModel(bootstrapJars, systemJars, statelessJars, extensions) =>
            sparkConf.set("spark.wasp.nifi.lib.stateless", statelessJars)
            sparkConf.set("spark.wasp.nifi.lib.bootstrap", bootstrapJars)
            sparkConf.set("spark.wasp.nifi.lib.system", systemJars)
            sparkConf.set("spark.wasp.nifi.lib.extensions", extensions)

            val newPlugins = Option(sparkConf.get("spark.executor.plugins", ""))
              .filterNot(_.isEmpty)
              .map(_ + ",it.agilelab.bigdata.wasp.spark.plugins.nifi.NifiPlugin")
              .getOrElse("it.agilelab.bigdata.wasp.spark.plugins.nifi.NifiPlugin")
            sparkConf.set("spark.executor.plugins", newPlugins)
        }
      case _ =>
    }

    logger.info(s"Resulting SparkConf:\n\t${sparkConf.toDebugString.replace("\n", "\n\t")}")

    sparkConf
  }

  private def getAdditionalJars(additionalJarsPath: String): Seq[String] = {

    val source = Source
      .fromFile(additionalJarsPath + File.separator + jarsListFileName)

    try {
      val additionalJars = source
        .getLines()
        .map(jarName => UtilsForwarder.resolveURI(additionalJarsPath + File.separator + jarName))
        .map(_.toString)
        .toVector

      additionalJars
    } catch {
      case e: Throwable =>
        val msg = s"Unable to completely generate the additional jars list - Exception: ${e.getMessage}"
        logger.error(msg, e)
        throw e
    }finally {
      source.close()
    }
  }

  def generateSpecificStructuredStreamingCheckpointDir(
      pipegraph: PipegraphModel,
      component: StructuredStreamingETLModel
  ): String = {

    val prefix = if (waspConfig.environmentPrefix == "") "" else "/" + waspConfig.environmentPrefix

    sparkStreamingConfig.checkpointDir + prefix + "/" +
      "structured_streaming" + "/" +
      pipegraph.generateStandardPipegraphName + "/" +
      component.generateStandardProcessingComponentName + "_" + component.generateStandardWriterName
  }

  def getTriggerIntervalMs(
      sparkStreamingConfigModel: SparkStreamingConfigModel,
      structuredStreamingETLModel: StructuredStreamingETLModel
  ): Long = {
    // grab the trigger interval from the etl, or if not specified from the config, or if not specified use 0
    structuredStreamingETLModel.triggerIntervalMs
      .orElse(sparkStreamingConfigModel.triggerIntervalMs)
      .getOrElse(0L) // this is the same default that Spark uses, see org.apache.spark.sql.streaming.DataStreamWriter
  }
}
