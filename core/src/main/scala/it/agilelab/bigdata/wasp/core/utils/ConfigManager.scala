package it.agilelab.bigdata.wasp.core.utils

import java.text.SimpleDateFormat
import java.util.Date
import java.util.Map.Entry

import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigValue}
import it.agilelab.bigdata.wasp.core.models.Model
import it.agilelab.bigdata.wasp.core.models.configuration._
import it.agilelab.bigdata.wasp.core.utils.ConfigManager.{getEntryConfigModel}
import org.bson.BsonString

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object ConfigManager {
  var conf: Config = ConfigFactory.load.getConfig("wasp") // grab the "wasp" subtree, as everything we need is in that namespace

  val kafkaConfigName = "Kafka"
  val sparkBatchConfigName = "SparkBatch"
  val sparkStreamingConfigName = "SparkStreaming"
  val elasticConfigName = "Elastic"
  val solrConfigName = "Solr"
  val hbaseConfigName = "HBase"
  val jdbcConfigName = "Jdbc"

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
    kafkaConfig =
      retrieveConf[KafkaConfigModel](getDefaultKafkaConfig, kafkaConfigName).get
  }

  private def getDefaultKafkaConfig: KafkaConfigModel = {
    val kafkaSubConfig = conf.getConfig("kafka")
    KafkaConfigModel(
      readConnectionsConfig(kafkaSubConfig, "connections"),
      kafkaSubConfig.getString("ingest-rate"),
      readZookeeperConnectionsConfig(kafkaSubConfig),
      kafkaSubConfig.getString("broker-id"),
      kafkaSubConfig.getString("partitioner-fqcn"),
      kafkaSubConfig.getString("default-encoder"),
      kafkaSubConfig.getString("key-encoder-fqcn"),
      kafkaSubConfig.getString("encoder-fqcn"),
      kafkaSubConfig.getString("decoder-fqcn"),
      kafkaSubConfig.getInt("batch-send-size"),
      getEntryConfigModel(kafkaSubConfig, "other-configs").map(e => KafkaEntryConfigModel(e._1, e._2)),
      kafkaConfigName
    )
  }

  def initializeSparkStreamingConfig(): Unit = {
    sparkStreamingConfig =
      retrieveConf[SparkStreamingConfigModel](getDefaultSparkStreamingConfig, sparkStreamingConfigName).get
  }

  private def getDefaultSparkStreamingConfig: SparkStreamingConfigModel = {
    val sparkSubConfig = conf.getConfig("spark-streaming")

    SparkStreamingConfigModel(
      sparkSubConfig.getString("app-name"),
      readConnectionConfig(sparkSubConfig.getConfig("master")),
      readSparkDriverConf(sparkSubConfig.getConfig("driver-conf")),
      sparkSubConfig.getInt("executor-cores"),
      sparkSubConfig.getString("executor-memory"),
      sparkSubConfig.getInt("executor-instances"),
      sparkSubConfig.getString("additional-jars-path"),
      sparkSubConfig.getString("yarn-jar"),
      sparkSubConfig.getInt("block-manager-port"),
      sparkSubConfig.getInt("retained-stages-jobs"),
      sparkSubConfig.getInt("retained-tasks"),
      sparkSubConfig.getInt("retained-jobs"),
      sparkSubConfig.getInt("retained-executions"),
      sparkSubConfig.getInt("retained-batches"),
      readKryoSerializerConfig(sparkSubConfig.getConfig("kryo-serializer")),

      sparkSubConfig.getInt("streaming-batch-interval-ms"),
      sparkSubConfig.getString("checkpoint-dir"),
      getEntryConfigModel(sparkSubConfig, "others").map(e => SparkEntryConfigModel(e._1, e._2)),

      sparkStreamingConfigName
    )
  }

  def initializeSparkBatchConfig(): Unit = {
    sparkBatchConfig =
      retrieveConf[SparkBatchConfigModel](getDefaultSparkBatchConfig, sparkBatchConfigName).get
  }

  private def getDefaultSparkBatchConfig: SparkBatchConfigModel = {
    val sparkSubConfig = conf.getConfig("spark-batch")

    SparkBatchConfigModel(
      sparkSubConfig.getString("app-name"),
      readConnectionConfig(sparkSubConfig.getConfig("master")),
      readSparkDriverConf(sparkSubConfig.getConfig("driver-conf")),
      sparkSubConfig.getInt("executor-cores"),
      sparkSubConfig.getString("executor-memory"),
      sparkSubConfig.getInt("executor-instances"),
      sparkSubConfig.getString("additional-jars-path"),
      sparkSubConfig.getString("yarn-jar"),
      sparkSubConfig.getInt("block-manager-port"),
      sparkSubConfig.getInt("retained-stages-jobs"),
      sparkSubConfig.getInt("retained-tasks"),
      sparkSubConfig.getInt("retained-jobs"),
      sparkSubConfig.getInt("retained-executions"),
      sparkSubConfig.getInt("retained-batches"),
      readKryoSerializerConfig(sparkSubConfig.getConfig("kryo-serializer")),
      getEntryConfigModel(sparkSubConfig, "others").map(e => SparkEntryConfigModel(e._1, e._2)),
      sparkBatchConfigName
    )
  }

  private def initializeElasticConfig(): Unit = {
    elasticConfig =
      retrieveConf[ElasticConfigModel](getDefaultElasticConfig, elasticConfigName).get
  }

  private def getDefaultElasticConfig: ElasticConfigModel = {
    val elasticSubConfig = conf.getConfig("elastic")
    ElasticConfigModel(
      readConnectionsConfig(elasticSubConfig, "connections"),
      elasticConfigName
    )
  }

  private def initializeSolrConfig(): Unit = {
    solrConfig =
      retrieveConf[SolrConfigModel](getDefaultSolrConfig, solrConfigName).get
  }

  private def getDefaultSolrConfig: SolrConfigModel = {
    val solrSubConfig = conf.getConfig("solrcloud")
    SolrConfigModel(
      readZookeeperConnectionsConfig(solrSubConfig),
      solrConfigName
    )
  }

  private def initializeHBaseConfig(): Unit = {
    hbaseConfig =
      retrieveConf[HBaseConfigModel](getDefaultHBaseConfig, hbaseConfigName).get
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
    jdbcConfig =
      retrieveConf[JdbcConfigModel](getDefaultJdbcConfig, jdbcConfigName).get
  }

  private def getDefaultJdbcConfig: JdbcConfigModel = {
    val jdbcSubConfig = conf.getConfig("jdbc")
    val connectionsMap = jdbcSubConfig
                            .getObject("connections")
                            .entrySet()
                            .asScala
                            .toSeq
                            .filter(entry => entry.getValue.isInstanceOf[ConfigObject])
                            .map(entry => (entry.getKey, entry.getValue.asInstanceOf[ConfigObject].toConfig))
                            .map(t => (t._1, readJdbcConfig(t._1, t._2)))
                            .toMap
    JdbcConfigModel(
      connectionsMap,
      jdbcConfigName
    )
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

  private def readJdbcConfig(name: String, config: Config): JdbcConnectionConfig = {
    JdbcConnectionConfig(
      name,
      config.getString("url"),
      config.getString("user"),
      config.getString("password"),
      config.getString("driverName")
    )
  }

  private def readConnectionsConfig(config: Config, path: String): Array[ConnectionConfig] = {
    val connections = config.getConfigList(path).asScala
    connections.map(connection => readConnectionConfig(connection)).toArray
  }

  private def readZookeeperConnectionsConfig(config: Config): ZookeeperConnectionsConfig = {
    val connectionsArray = readConnectionsConfig(config, "zookeeperConnections")
    val chRoot = config.getString("zkChRoot")
    ZookeeperConnectionsConfig(connectionsArray, chRoot)
  }

  private def readConnectionConfig(config: Config): ConnectionConfig = {
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

  private def readSparkDriverConf(config: Config): SparkDriverConfig = {

    SparkDriverConfig(
      config.getString("submit-deploy-mode"),
      config.getInt("driver-cores"),
      config.getString("driver-memory"),
      config.getString("driver-hostname"),
      config.getString("driver-bind-address"),
      config.getInt("driver-port")
    )
  }

  private def readKryoSerializerConfig(config: Config): KryoSerializerConfig = {

    KryoSerializerConfig(
      config.getBoolean("enabled"),
      config.getString("registrators"),
      config.getBoolean("strict")
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

  def getEntryConfigModel(config: Config, key: String):  Seq[(String, String)] = {
    if (config.hasPath(key)) {
      val list: Iterable[ConfigObject] = config.getObjectList(key).asScala
      (for {
        item: ConfigObject <- list
        entry: Entry[String, ConfigValue] <- item.entrySet().asScala
        key = entry.getKey
        value = entry.getValue.unwrapped().toString
      } yield (key, value)).toSeq
    } else {
      Seq()
    }
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

    result
  }
}

case class ZookeeperConnectionsConfig(connections: Seq[ConnectionConfig],
                                      chRoot: String) {

  override def toString = connections.map(conn => s"${conn.host}:${conn.port}").mkString(",") + s"${chRoot}"
}

case class SparkDriverConfig(submitDeployMode: String,
                             cores: Int,
                             memory: String,
                             host: String,
                             bindAddress: String,
                             port: Int)

case class KryoSerializerConfig(enabled: Boolean,
                                registrators: String,
                                strict: Boolean)

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
