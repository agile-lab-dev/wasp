package it.agilelab.bigdata.wasp.core.utils

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import it.agilelab.bigdata.wasp.core.models.BSONConversionHelper

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.runtime.universe.TypeTag
import reactivemongo.api.commands._
import com.typesafe.config.{ConfigObject, ConfigValue, ConfigFactory, Config}
import scala.collection.JavaConverters._
import java.net.URI
import java.util.Map.Entry
import reactivemongo.bson.{BSONDocumentReader, BSONDocumentWriter, BSONString}
import it.agilelab.bigdata.wasp.core.models.configuration._

object ConfigManager extends BSONConversionHelper {
  var conf: Config = ConfigFactory.load

  val kafkaConfigName = "Kafka"
  val sparkBatchConfigName = "SparkBatch"
  val sparkStreamingConfigName = "SparkStreaming"
  val elasticConfigName = "Elastic"
  val solrConfigName = "Solr"

  var mongoDBConfig: MongoDBConfigModel = _
  var kafkaConfig: KafkaConfigModel = _
  var sparkBatchConfig: SparkBatchConfigModel = _
  var sparkStreamingConfig: SparkStreamingConfigModel = _
  var elasticConfig: ElasticConfigModel = _
  var solrConfig: SolrConfigModel = _

  private def initializeMongoConfig(): Unit = {
    mongoDBConfig = getDefaultMongoConfig // mongo config is always read from file, so it's always "default"
  }

  private def getDefaultMongoConfig: MongoDBConfigModel = {
    val mongoSubConfig = conf.getConfig("mongo")
    MongoDBConfigModel(
      mongoSubConfig.getString("address"),
      mongoSubConfig.getString("db-name"),
      mongoSubConfig.getInt("timeout")
    )
  }

  private def initializeKafkaConfig(): Unit = {
    kafkaConfig = retrieveConf(getDefaultKafkaConfig, kafkaConfigName).get
  }

  private def getDefaultKafkaConfig: KafkaConfigModel = {
    val kafkaSubConfig = conf.getConfig("kafka")
    KafkaConfigModel(
      readConnections(kafkaSubConfig, "connections"),
      kafkaSubConfig.getString("ingest-rate"),
      readConnection(kafkaSubConfig.getConfig("zookeeper")),
      kafkaSubConfig.getString("broker-id"),
      kafkaSubConfig.getString("partitioner-fqcn"),
      kafkaSubConfig.getString("default-encoder"),
      kafkaSubConfig.getString("encoder-fqcn"),
      kafkaSubConfig.getString("decoder-fqcn"),
      kafkaSubConfig.getInt("batch-send-size"),
      kafkaConfigName
    )
  }

  private def initializeSparkBatchConfig(): Unit = {
    sparkBatchConfig =
      retrieveConf(getDefaultSparkBatchConfig, sparkBatchConfigName).get
  }

  private def getDefaultSparkBatchConfig: SparkBatchConfigModel = {
    val sparkSubConfig = conf.getConfig("spark-batch")
    SparkBatchConfigModel(
      sparkSubConfig.getString("app-name"),
      readConnection(sparkSubConfig.getConfig("master")),
      sparkSubConfig.getInt("driver-cores"),
      sparkSubConfig.getString("driver-memory"),
      sparkSubConfig.getString("driver-hostname"),
      sparkSubConfig.getInt("driver-port"),
      sparkSubConfig.getInt("executor-cores"),
      sparkSubConfig.getString("executor-memory"),
      sparkSubConfig.getInt("executor-instances"),
      None, // no sensible default; must be filled in while instantiating SparkContext
      sparkSubConfig.getString("yarn-jar"),
      sparkSubConfig.getInt("block-manager-port"),
      sparkSubConfig.getInt("broadcast-port"),
      sparkSubConfig.getInt("fileserver-port"),
      sparkBatchConfigName
    )
  }

  private def initializeSparkStreamingConfig(): Unit = {
    sparkStreamingConfig = retrieveConf(getDefaultSparkStreamingConfig,
                                        sparkStreamingConfigName).get
  }

  private def getDefaultSparkStreamingConfig: SparkStreamingConfigModel = {
    val sparkSubConfig = conf.getConfig("spark-streaming")
    SparkStreamingConfigModel(
      sparkSubConfig.getString("app-name"),
      readConnection(sparkSubConfig.getConfig("master")),
      sparkSubConfig.getInt("driver-cores"),
      sparkSubConfig.getString("driver-memory"),
      sparkSubConfig.getString("driver-hostname"),
      sparkSubConfig.getInt("driver-port"),
      sparkSubConfig.getInt("executor-cores"),
      sparkSubConfig.getString("executor-memory"),
      sparkSubConfig.getInt("executor-instances"),
      None, // no sensible default; must be filled in while instantiating SparkContext
      sparkSubConfig.getString("yarn-jar"),
      sparkSubConfig.getInt("block-manager-port"),
      sparkSubConfig.getInt("broadcast-port"),
      sparkSubConfig.getInt("fileserver-port"),
      sparkSubConfig.getInt("streaming-batch-interval-ms"),
      sparkSubConfig.getString("checkpoint-dir"),
      sparkStreamingConfigName
    )
  }

  private def initializeElasticConfig(): Unit = {
    elasticConfig =
      retrieveConf(getDefaultElasticConfig, elasticConfigName).get
  }

  private def initializeSolrConfig(): Unit = {
    solrConfig = retrieveConf(getDefaultSolrConfig, solrConfigName).get
  }

  private def getDefaultElasticConfig: ElasticConfigModel = {
    val elasticSubConfig = conf.getConfig("elastic")
    ElasticConfigModel(
      readConnections(elasticSubConfig, "connections"),
      elasticSubConfig.getString("cluster-name"),
      elasticConfigName
    )
  }

  private def getDefaultSolrConfig: SolrConfigModel = {
    val solrSubConfig = conf.getConfig("solrcloud")
    SolrConfigModel(
      readConnections(solrSubConfig, "connections"),
      Some(readApiEndPoint(solrSubConfig, "apiEndPoint")),
      solrConfigName,
      None,
      "wasp"
    )
  }

  /**
    * Initialize the configurations managed by this ConfigManager.
    */
  def initializeConfigs(): Unit = {
    initializeKafkaConfig()
    initializeSparkBatchConfig()
    initializeSparkStreamingConfig()
    initializeElasticConfig()
    initializeSolrConfig()
  }

  /**
    * Re-initialize the config manager, reading from file.
    *
    * @param file
    */
  def reloadConfig(file: File): Unit = {
    conf = ConfigFactory.parseFile(file)
    initializeConfigs()
  }

  /** Re-initialize the config manager.
    */
  def reloadConfig(): Unit = {
    conf = ConfigFactory.load()
    initializeConfigs()
  }

  def getKafkaConfig = {
    if (kafkaConfig == null) {
      throw new Exception("The kafka configuration was not initialized")
    }
    kafkaConfig
  }

  def getSparkBatchConfig = {
    if (sparkBatchConfig == null) {
      throw new Exception("The spark batch configuration was not initialized")
    }
    sparkBatchConfig
  }

  def getSparkStreamingConfig = {
    if (sparkStreamingConfig == null) {
      throw new Exception(
        "The spark streaming configuration was not initialized")
    }
    sparkStreamingConfig
  }

  def getElasticConfig = {
    if (elasticConfig == null) {
      throw new Exception("The elastic configuration was not initialized")
    }
    elasticConfig
  }

  def getSolrConfig = {
    if (solrConfig == null) {
      throw new Exception("The solr configuration was not initialized")
    }
    solrConfig
  }

  def getMongoDBConfig = {
    if (mongoDBConfig == null) {
      initializeMongoConfig()
    }
    mongoDBConfig
  }

  private def readConnections(config: Config,
                              path: String): Array[ConnectionConfig] = {
    val connections = config.getConfigList(path).asScala
    connections map (connection => readConnection(connection)) toArray
  }

  private def readApiEndPoint(config: Config, path: String): ConnectionConfig = {
    val connection = config.getConfig(path)
    readConnection(connection)
  }

  private def readConnection(config: Config) = {
    val timeout = if (config.hasPath("timeout")) {
      Some(config.getLong("timeout"))
    } else {
      None
    }

    val metadata = if (config.hasPath("metadata")) {
      val list: Iterable[ConfigObject] =
        config.getObjectList("metadata").asScala
      val md = (for {
        item: ConfigObject <- list
        entry: Entry[String, ConfigValue] <- item.entrySet().asScala
        key = entry.getKey
        value = entry.getValue.unwrapped().toString
      } yield (key, value)).toMap
      Some(md)
    } else {
      None
    }

    ConnectionConfig(
      config.getString("protocol"),
      config.getString("host"),
      config.getInt("port"),
      timeout,
      metadata
    )
  }

  /**
    * Read the configuration with the specified name from MongoDB or, if it is not present, initialize
    * it with the provided defaults.
		*/
  private def retrieveConf[T](default: T, nameConf: String)(
      implicit typeTag: TypeTag[T],
      swriter: BSONDocumentWriter[T],
      sreader: BSONDocumentReader[T]): Option[T] = {
    val document =
      WaspDB.getDB.getDocumentByField[T]("name", new BSONString(nameConf))
    val awaitedDoc = Await.result(document, 5.seconds)

    //val result = document.flatMap(x => if (x.isEmpty) insert[T](default).map { x => Some(default) } else future { Some(default) })
    // A few more line of code but now is working as intended.
    if (awaitedDoc.isEmpty) {
      WaspDB.getDB
        .insert[T](default)
        .map(e => println(WriteResult.lastError(e).flatMap(_.errmsg)))
      Some(default)
    } else {
      awaitedDoc
    }
  }

  def buildTimedName(prefix: String): String = {
    val result = prefix + "-" + new SimpleDateFormat("yyyy.MM.dd")
        .format(new Date())

    result
  }
}

case class ConnectionConfig(protocol: String,
                            host: String,
                            port: Int = 0,
                            timeout: Option[Long] = None,
                            metadata: Option[Map[String, String]]) {

  override def toString = {
    var result = ""

    if (protocol != null && !protocol.isEmpty)
      result = protocol + "://"

    result = result + host

    if (port != 0)
      result = result + ":" + port

    result + metadata.flatMap(_.get("zookeeperRootNode")).getOrElse("")
  }
}

trait SparkBatchConfiguration {
  lazy val sparkBatchConfig = ConfigManager.getSparkBatchConfig
}

trait SparkStreamingConfiguration {
  lazy val sparkStreamingConfig = ConfigManager.getSparkStreamingConfig
}

trait ElasticConfiguration {
  lazy val elasticConfig = ConfigManager.getElasticConfig
}

trait SolrConfiguration {
  lazy val solrConfig = ConfigManager.getSolrConfig
}
