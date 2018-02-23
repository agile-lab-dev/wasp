package it.agilelab.bigdata.wasp.core.utils

import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Map.Entry

import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigValue}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.Model
import it.agilelab.bigdata.wasp.core.models.configuration._
import org.bson.BsonString

import scala.collection.JavaConverters._
import scala.io.Source
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

object ConfigManager extends Logging {
  var conf: Config = ConfigFactory.load.getConfig("wasp") // grab the "wasp" subtree, as everything we need is in that namespace

  val kafkaConfigName = "Kafka"
  val sparkBatchConfigName = "SparkBatch"
  val sparkStreamingConfigName = "SparkStreaming"
  val elasticConfigName = "Elastic"
  val solrConfigName = "Solr"
  val hbaseConfigName = "HBase"
  val jdbcConfigName = "Jdbc"
  val waspConsumersSparkAdditionalJarsFileName = "wasp-consumers-spark-additional-jars-path-names"

  private var waspConfig : WaspConfigModel = _
  private var mongoDBConfig: MongoDBConfigModel = _
  private var kafkaConfig: KafkaConfigModel = _
  private var sparkBatchConfig: SparkBatchConfigModel = _
  private var sparkStreamingConfig: SparkStreamingConfigModel = _
  private var elasticConfig: ElasticConfigModel = _
  private var solrConfig: SolrConfigModel = _
  private var hbaseConfig: HBaseConfigModel = _
  private var jdbcConfig: JdbcConfigModel = _
  
  private def initializeWaspConfig(): Unit = {
    waspConfig = getDefaultWaspConfig // wasp config is always read from file, so it's always "default"
  }
  
  private def getDefaultWaspConfig: WaspConfigModel = {
    WaspConfigModel(
      conf.getString("actor-system-name"),
      conf.getInt("actor-downing-timeout-millis"),
      conf.getBoolean("index-rollover"),
      conf.getInt("general-timeout-millis"),
      conf.getInt("services-timeout-millis"),
      conf.getString("additional-jars-path"),
      conf.getString("datastore.indexed"),
      conf.getString("datastore.keyvalue"),
      conf.getBoolean("systempipegraphs.start"),
      conf.getBoolean("systemproducers.start"),
      conf.getString("rest.server.hostname"),
      conf.getInt("rest.server.port"),
      conf.getString("environment.prefix")
    )
  }
  
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
    kafkaConfig = retrieveConf[KafkaConfigModel](getDefaultKafkaConfig, kafkaConfigName).get
  }

  private def getDefaultKafkaConfig: KafkaConfigModel = {
    val kafkaSubConfig = conf.getConfig("kafka")
    KafkaConfigModel(
      readConnections(kafkaSubConfig, "connections"),
      kafkaSubConfig.getString("ingest-rate"),
      readZookeeperConnections(kafkaSubConfig, "zookeeperConnections", "zkChRoot"),
      kafkaSubConfig.getString("broker-id"),
      kafkaSubConfig.getString("partitioner-fqcn"),
      kafkaSubConfig.getString("default-encoder"),
      kafkaSubConfig.getString("encoder-fqcn"),
      kafkaSubConfig.getString("decoder-fqcn"),
      kafkaSubConfig.getInt("batch-send-size"),
      kafkaConfigName
    )
  }

  def initializeSparkBatchConfig(): Unit = {
    sparkBatchConfig = retrieveConf(getDefaultSparkBatchConfig,
                                    sparkBatchConfigName).get
  }

  private def getDefaultSparkBatchConfig: SparkBatchConfigModel = {
    val sparkSubConfig = conf.getConfig("spark-batch")

    val additionalJars = getAdditionalJars

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
      additionalJars,
      sparkSubConfig.getString("yarn-jar"),
      sparkSubConfig.getInt("block-manager-port"),
      sparkSubConfig.getInt("broadcast-port"),
      sparkSubConfig.getInt("fileserver-port"),
      sparkBatchConfigName,
      sparkSubConfig.getString("driver-bind-address"),
      sparkSubConfig.getInt("retained-stages-jobs"),
      sparkSubConfig.getInt("retained-tasks"),
      sparkSubConfig.getInt("retained-executions"),
      sparkSubConfig.getInt("retained-batches")
    )
  }

  private def getAdditionalJars: Option[Seq[String]] = {
    scala.util.Try{
      val additionalJarsPath = conf.getString("additional-jars-lib-path")
      val additionalJars = Source.fromFile(additionalJarsPath.concat(waspConsumersSparkAdditionalJarsFileName))
        .getLines()
        .map(name => URLEncoder.encode(additionalJarsPath.concat(name), "UTF-8"))
        .toSeq

      additionalJars
    } match {
      case Success(result) => Some(result)
      case Failure(_) => None
    }
  }

  def initializeSparkStreamingConfig(): Unit = {
    sparkStreamingConfig = retrieveConf[SparkStreamingConfigModel](getDefaultSparkStreamingConfig,
                                        sparkStreamingConfigName).get
  }

  private def getDefaultSparkStreamingConfig: SparkStreamingConfigModel = {
    val sparkSubConfig = conf.getConfig("spark-streaming")

    logger.info(sparkSubConfig.toString)

    val additionalJars = getAdditionalJars

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
      additionalJars,
      sparkSubConfig.getString("yarn-jar"),
      sparkSubConfig.getInt("block-manager-port"),
      sparkSubConfig.getInt("broadcast-port"),
      sparkSubConfig.getInt("fileserver-port"),
      sparkSubConfig.getInt("streaming-batch-interval-ms"),
      sparkSubConfig.getString("checkpoint-dir"),
      sparkStreamingConfigName,
      sparkSubConfig.getString("driver-bind-address"),
      sparkSubConfig.getInt("retained-stages-jobs"),
      sparkSubConfig.getInt("retained-tasks"),
      sparkSubConfig.getInt("retained-executions"),
      sparkSubConfig.getInt("retained-batches")
    )
  }

  private def initializeElasticConfig(): Unit = {
    elasticConfig =
      retrieveConf[ElasticConfigModel](getDefaultElasticConfig, elasticConfigName).get
  }

  private def initializeSolrConfig(): Unit = {
    solrConfig = retrieveConf[SolrConfigModel](getDefaultSolrConfig, solrConfigName).get
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
      readZookeeperConnections(solrSubConfig, "zookeeperConnections", "zkChRoot"),
      Some(readApiEndPoint(solrSubConfig, "apiEndPoint")),
      solrConfigName,
      "wasp"
    )
  }
  
  private def initializeHBaseConfig(): Unit = {
    hbaseConfig = retrieveConf[HBaseConfigModel](getDefaultHBaseConfig, hbaseConfigName).get
  }
  
  private def getDefaultHBaseConfig: HBaseConfigModel = {
    val hbaseSubConfig = conf.getConfig("hbase")
    HBaseConfigModel(
      hbaseSubConfig.getString("core-site-xml-path"),
      hbaseSubConfig.getString("hbase-site-xml-path"),
      hbaseConfigName
    )
  }

  private def initializeJdbcConfig(): Unit = {
    jdbcConfig = retrieveConf[JdbcConfigModel](getDefaultJdbcConfig, jdbcConfigName).get
  }

  private def getDefaultJdbcConfig: JdbcConfigModel = {
    val jdbcSubConfig = conf.getConfig("jdbc")
    JdbcConfigModel(
      jdbcConfigName,//name
      jdbcSubConfig.getString("dbType"),
      jdbcSubConfig.getString("host"),
      jdbcSubConfig.getInt("port"),
      jdbcSubConfig.getString("user"),//user
      jdbcSubConfig.getString("password"),//password
      jdbcSubConfig.getString("driverName"),//driverName
      readJdbcPartitioningInfo(jdbcSubConfig, "partitioningInfo"), //partitioningInfo
      if(jdbcSubConfig.hasPath("numPartitions")) Some(jdbcSubConfig.getInt("numPartitions")) else None,//numPartitions
      if(jdbcSubConfig.hasPath("fetchSize")) Some(jdbcSubConfig.getInt("fetchSize")) else None//fetchSize
    )
  }

  private def readJdbcPartitioningInfo(jdbcConf: Config, path: String): Option[JdbcPartitioningInfo] = {
    if(jdbcConf.hasPath(path)) {
      val partInfoConf = jdbcConf.getConfig(path)
      Some(
        JdbcPartitioningInfo(
            partInfoConf.getString("partitionColumn"),
            partInfoConf.getString("lowerBound"),
            partInfoConf.getString("upperBound")
          )
      )
    }
    else None
  }

  /**
    * Initialize the configurations managed by this ConfigManager.
    */
  def initializeCommonConfigs(): Unit = {
    initializeWaspConfig()
    initializeMongoConfig()
    initializeKafkaConfig()
    initializeElasticConfig()
    initializeSolrConfig()
    initializeHBaseConfig()
    initializeJdbcConfig()
    initializeSparkStreamingConfig()
    initializeSparkBatchConfig()
  }
  
  def getWaspConfig: WaspConfigModel = {
    if (waspConfig == null) {
      initializeWaspConfig()
    }
    waspConfig
  }
  
  def getMongoDBConfig: MongoDBConfigModel = {
    if (mongoDBConfig == null) {
      initializeMongoConfig()
    }
    mongoDBConfig
  }
  
  def getKafkaConfig: KafkaConfigModel = {
    if (kafkaConfig == null) {
      throw new Exception("The kafka configuration was not initialized")
    }
    kafkaConfig
  }

  def getSparkBatchConfig: SparkBatchConfigModel = {
    if (sparkBatchConfig == null) {
      throw new Exception("The spark batch configuration was not initialized")
    }
    sparkBatchConfig
  }

  def getSparkStreamingConfig: SparkStreamingConfigModel = {
    if (sparkStreamingConfig == null) {
      throw new Exception(
        "The spark streaming configuration was not initialized")
    }
    sparkStreamingConfig
  }

  def getElasticConfig: ElasticConfigModel = {
    if (elasticConfig == null) {
      throw new Exception("The elastic configuration was not initialized")
    }
    elasticConfig
  }

  def getSolrConfig: SolrConfigModel = {
    if (solrConfig == null) {
      throw new Exception("The solr configuration was not initialized")
    }
    solrConfig
  }

  def getHBaseConfig: HBaseConfigModel = {
    if (hbaseConfig == null) {
      throw new Exception("The hbase configuration was not initialized")
    }
    hbaseConfig
  }

  def getJdbcConfig: JdbcConfigModel = {
    if (jdbcConfig == null) {
      throw new Exception("The jdbc configuration was not initialized")
    }
    jdbcConfig
  }

  private def readConnections(config: Config,
                              path: String): Array[ConnectionConfig] = {
    val connections = config.getConfigList(path).asScala
    connections map (connection => readConnection(connection)) toArray
  }


  private def readZookeeperConnections(config: Config,
                                       zkPath: String,
                                       zkPathChRoot: String): ZookeeperConnection = {

    val connections = config.getConfigList(zkPath).asScala
    val chRoot = Try {config.getString(zkPathChRoot)}.toOption
    val connectionsArray = connections.map(connection => readConnection(connection)).toArray
    ZookeeperConnection(connectionsArray, chRoot)
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
  private def retrieveConf[T <: Model](default: T, nameConf: String)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[T] = {
    WaspDB.getDB.insertIfNotExists[T](default)

    return WaspDB.getDB.getDocumentByField[T]("name", new BsonString(nameConf))
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

case class ZookeeperConnection(connections: Seq[ConnectionConfig],
                               chRoot: Option[String]) {
  def getZookeeperConnection() = {
    connections.map(conn => s"${conn.host}:${conn.port}").mkString(",") + s"${chRoot.getOrElse("")}"
  }
}

trait WaspConfiguration {
  lazy val waspConfig: WaspConfigModel = ConfigManager.getWaspConfig
}

trait MongoDBConfiguration {
  lazy val mongoDBConfig: MongoDBConfigModel = ConfigManager.getMongoDBConfig
}

trait SparkBatchConfiguration {
  lazy val sparkBatchConfig: SparkBatchConfigModel = ConfigManager.getSparkBatchConfig
}

trait SparkStreamingConfiguration {
  lazy val sparkStreamingConfig: SparkStreamingConfigModel = ConfigManager.getSparkStreamingConfig
}

trait ElasticConfiguration {
  lazy val elasticConfig: ElasticConfigModel = ConfigManager.getElasticConfig
}

trait SolrConfiguration {
  lazy val solrConfig: SolrConfigModel = ConfigManager.getSolrConfig
}

trait HBaseConfiguration {
  lazy val hbaseConfig: HBaseConfigModel = ConfigManager.getHBaseConfig
}

trait JdbcConfiguration {
  lazy val jdbcConfig: JdbcConfigModel = ConfigManager.getJdbcConfig
}
