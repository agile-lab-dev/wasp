package it.agilelab.bigdata.wasp.core.utils

import java.nio.ByteOrder
import java.util.Map.Entry

import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigValue, ConfigValueFactory}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.repository.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.models.Model
import it.agilelab.bigdata.wasp.models.configuration._

import it.agilelab.darwin.manager.util.ConfigurationKeys

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object ConfigManager extends Logging with CanOverrideNameInstances {
  val conf: Config = ConfigFactory.load.getConfig("wasp") // grab the "wasp" subtree, as everything we need is in that namespace

  val kafkaConfigName = "Kafka"
  val sparkBatchConfigName = "SparkBatch"
  val sparkStreamingConfigName = "SparkStreaming"
  val elasticConfigName = "Elastic"
  val solrConfigName = "Solr"
  val hbaseConfigName = "HBase"
  val jdbcConfigName = "Jdbc"
  val telemetryConfigName = "Telemetry"
  val nifiConfigName = "Nifi"
  val compilerConfigName = "Compiler"

  private val globalValidationRules: Seq[ValidationRule] = Seq(
    /* sparkStreamingConfig validation-rules */

    ValidationRule("SparkStreamingStandaloneMode") { (configManager) =>
      if (configManager.getSparkStreamingConfig.master.protocol == "spark") {
        if (configManager.getSparkStreamingConfig.coresMax < configManager.getSparkStreamingConfig.executorCores)
          Left("Running on YARN without specifying spark.yarn.jar is unlikely to work!")
        else
          Right(())
      } else
        Right(())
    },
    ValidationRule("SparkStreamingYARNmode") { (configManager) =>
      val master = configManager.getSparkStreamingConfig.master
      if (master.protocol == "" && master.host == "yarn") {
        if (configManager.getSparkStreamingConfig.yarnJar.isEmpty)
          Left("Running in YARN mode without specifying 'spark.yarn.jar' is unlikely to work!")
        else
          Right(())
      } else
        Right(())
    },
    ValidationRule("SparkStreamingCheckpointDirLocal") { (configManager) =>
      if (configManager.getSparkStreamingConfig.checkpointDir.startsWith("file:///"))
        Left(
          "Using a localPath (within the consumers-spark-streaming container) for the checkpoint directory is not recommended. Use a remotePath on HDFS (i.e. '/...') instead"
        )
      else
        Right(())
    },
    /* sparkBatchConfig validation-rules */

    ValidationRule("SparkBatchStandaloneMode") { (configManager) =>
      if (configManager.getSparkBatchConfig.master.protocol == "spark") {
        if (configManager.getSparkBatchConfig.coresMax < configManager.getSparkBatchConfig.executorCores)
          Left("Running on YARN without specifying spark.yarn.jar is unlikely to work!")
        else
          Right(())
      } else
        Right(())
    },
    ValidationRule("SparkBatchYARNmode") { (configManager) =>
      val master = configManager.getSparkBatchConfig.master
      if (master.protocol == "" && master.host == "yarn") {
        if (configManager.getSparkBatchConfig.yarnJar.isEmpty)
          Left("Running in YARN mode without specifying 'spark.yarn.jar' is unlikely to work!")
        else
          Right(())
      } else
        Right(())
    }
  )

  private var waspConfig: WaspConfigModel = _
  private var telemetryConfig: TelemetryConfigModel = _
  private var mongoDBConfig: MongoDBConfigModel = _
  private var pgDBConfig: PostgresDBConfigModel = _
  private var kafkaConfig: KafkaConfigModel = _
  private var sparkBatchConfig: SparkBatchConfigModel = _
  private var sparkStreamingConfig: SparkStreamingConfigModel = _
  private var elasticConfig: ElasticConfigModel = _
  private var solrConfig: SolrConfigModel = _
  private var hbaseConfig: HBaseConfigModel = _
  private var jdbcConfig: JdbcConfigModel = _
  private var avroSchemaManagerConfig: Config = _
  private var nifiConfig: NifiConfigModel = _
  private var compilerConfig: CompilerConfigModel = _

  def validateConfigs(pluginsValidationRules: Seq[ValidationRule] = Seq()): Map[String, Either[String, Unit]] = {
    (globalValidationRules ++ pluginsValidationRules)
      .filterNot(validationRule => waspConfig.validationRulesToIgnore.contains(validationRule.key))
      .map(validationRule => validationRule.key -> validationRule.func(this))
      .toMap
  }

  private def initializeWaspConfig(): Unit = {
    waspConfig = getDefaultWaspConfig // wasp config is always read from file, so it's always "default"
  }

  private def getDefaultWaspConfig: WaspConfigModel = {
    val environmentSubConfig = conf.getConfig("environment")
    WaspConfigModel(
      conf.getString("actor-system-name"),
      conf.getInt("actor-downing-timeout-millis"),
      conf.getBoolean("index-rollover"),
      conf.getInt("general-timeout-millis"),
      conf.getInt("services-timeout-millis"),
      conf.getBoolean("systempipegraphs.start"),
      conf.getBoolean("systemproducers.start"),
      conf.getString("rest.server.hostname"),
      conf.getInt("rest.server.port"),
      readRestHttpsConfig(),
      environmentSubConfig.getString("prefix"),
      readValidationRulesToIgnore(environmentSubConfig, "validationRulesToIgnore"),
      environmentSubConfig.getString("mode"),
      conf.getString("darwinConnector"),
      ConfigurationMode.fromString(conf.getString("configuration-mode")),
      scala.util.Try(conf.getStringList("rest.authentication.apiKeys").asScala).toOption
    )
  }

  def initializeAvroSchemaManagerConfig(): Unit = {

    val tmp = waspConfig.darwinConnector match {
      case "hbase" => ConfigFactory.parseMap(getAvroSchemaManagerConfigHbaseConnector.asJava)
      case _ => conf.getConfig("avroSchemaManager.darwin")
    }
    avroSchemaManagerConfig = if (!tmp.hasPath(ConfigurationKeys.ENDIANNESS)) {
      logger.warn(
        s"No ${ConfigurationKeys.ENDIANNESS} configured on Darwin, " +
          s"we are defaulting to ${ByteOrder.BIG_ENDIAN} for compatibility with old versions, but you should configure " +
          s"it explictly with either ${ByteOrder.BIG_ENDIAN} or  ${ByteOrder.LITTLE_ENDIAN}"
      )
      tmp.withValue(ConfigurationKeys.ENDIANNESS, ConfigValueFactory.fromAnyRef(ByteOrder.BIG_ENDIAN.toString))
    } else {
      tmp
    }

  }

  private def getAvroSchemaManagerConfigHbaseConnector: Map[String, AnyRef] = {

    val avroSchemaManagerSubConfig = conf.getConfig("avroSchemaManager")
    val darwinConfig = avroSchemaManagerSubConfig.getConfig("darwin")
    val defaultConf = darwinConfig.root().unwrapped().asScala.toMap

    if (avroSchemaManagerSubConfig.getBoolean("wasp-manages-darwin-connectors-conf")) {
      val env = System.getenv()

      val hbaseSubConfig = conf.getConfig("hbase")

      val isSecure: java.lang.Boolean =
        if (env.containsKey("WASP_SECURITY"))
          java.lang.Boolean.getBoolean(env.get("WASP_SECURITY"))
        else
          java.lang.Boolean.FALSE
      val principal = if (env.containsKey("PRINCIPAL_NAME")) env.get("PRINCIPAL_NAME") else ""
      val keytabPath = if (env.containsKey("KEYTAB_FILE_NAME")) env.get("KEYTAB_FILE_NAME") else ""

      defaultConf ++ Map(
        "namespace" -> darwinConfig.getString("namespace"),
        "table" -> darwinConfig.getString("table"),
        "hbaseSite" -> hbaseSubConfig.getString("hbase-site-xml-path"),
        "coreSite" -> hbaseSubConfig.getString("core-site-xml-path"),
        "isSecure" -> isSecure,
        "principal" -> principal,
        "keytabPath" -> keytabPath
      )
    } else {
      defaultConf
    }
  }

  private def namespaced[T: CanOverrideName](entity: T, name: String): (T, Option[String]) = {
    this.waspConfig.configurationMode match {
      case ConfigurationMode.Legacy =>
        (entity, Some(name))
      case ConfigurationMode.Local =>
        (entity, None)
      case ConfigurationMode.NamespacedConfigurations(namespace) =>
        val namespacedId = s"$namespace.$name"
        (implicitly[CanOverrideName[T]].named(entity, namespacedId), Some(namespacedId))
    }
  }

  private def initializeTelemetryConfig(): Unit = {
    telemetryConfig = retrieveConf[TelemetryConfigModel](getDefaultTelemetryConfig, telemetryConfigName).get
  }

  private def getDefaultTelemetryConfig: TelemetryConfigModel = {
    val telemetrySubConfig = conf.getConfig("telemetry")
    TelemetryConfigModel(
      name = telemetryConfigName,
      writer = telemetrySubConfig.getString("writer"),
      sampleOneMessageEvery = telemetrySubConfig.getInt("latency.sample-one-message-every"),
      telemetryTopicConfigModel = TelemetryTopicConfigModel(
        topicName = telemetrySubConfig.getString("topic.name"),
        partitions = telemetrySubConfig.getInt("topic.partitions"),
        replica = telemetrySubConfig.getInt("topic.replica"),
        readOthersConfig(telemetrySubConfig.getConfig("topic")).map(e => KafkaEntryConfig(e._1, e._2)),
        telemetrySubConfig.getConfigList("topic.jmx").asScala.map(readJmxTelemetryConfig)
      )
    )
  }

  private def readJmxTelemetryConfig(config: Config): JMXTelemetryConfigModel = {
    JMXTelemetryConfigModel(
      query = config.getString("query"),
      metricGroupAttribute = config.getString("metricGroupAttribute"),
      sourceIdAttribute = config.getString("sourceIdAttribute"),
      metricGroupFallback =
        if (config.hasPath("metricGroupFallback")) config.getString("metricGroupFallback") else "unknown",
      sourceIdFallback = if (config.hasPath("sourceIdFallback")) config.getString("metricGroupFallback") else "unknown"
    )
  }

  private def initializeMongoDBConfig(): Unit = {
    mongoDBConfig = getDefaultMongoDBConfig // mongoDB config is always read from file, so it's always "default"
  }

  private def getDefaultMongoDBConfig: MongoDBConfigModel = {
    val mongoDBSubConfig = conf.getConfig("mongo")
    MongoDBConfigModel(
      mongoDBSubConfig.getString("address"),
      mongoDBSubConfig.getString("db-name"),
      mongoDBSubConfig.getString("username"),
      mongoDBSubConfig.getString("password"),
      mongoDBSubConfig.getInt("timeout")
    )
  }

  private def initializePostgresDBConfig(): Unit = {
    pgDBConfig = getDefaultPostgresDBConfig // postgres config is always read from file, so it's always "default"
  }

  private def getDefaultPostgresDBConfig: PostgresDBConfigModel = {
    val pgDBSubConfig = conf.getConfig("postgres")
    PostgresDBConfigModel(
      pgDBSubConfig.getString("url"),
      pgDBSubConfig.getString("user"),
      pgDBSubConfig.getString("password"),
      pgDBSubConfig.getString("driver"),
      pgDBSubConfig.getInt("pool.size")
    )
  }

  private def initializeKafkaConfig(): Unit = {
    kafkaConfig = retrieveConf[KafkaConfigModel](getDefaultKafkaConfig, kafkaConfigName).get
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
      kafkaSubConfig.getString("acks"),
      readOthersConfig(kafkaSubConfig).map(e => KafkaEntryConfig(e._1, e._2)),
      kafkaConfigName
    )
  }

  def initializeSparkStreamingConfig(): Unit = {
    sparkStreamingConfig =
      retrieveConf[SparkStreamingConfigModel](getDefaultSparkStreamingConfig, sparkStreamingConfigName).get
  }

  def initializeCompilerConfig(): Unit = {
    compilerConfig = retrieveConf[CompilerConfigModel](getDefaultCompilerConfig, compilerConfigName).get
  }

  private def getDefaultCompilerConfig: CompilerConfigModel = {
    val compilerSubConfig = conf.getConfig("compiler")

    CompilerConfigModel(compilerSubConfig.getInt("max-instances"), compilerConfigName)
  }

  private def getDefaultSparkStreamingConfig: SparkStreamingConfigModel = {
    val sparkSubConfig = conf.getConfig("spark-streaming")

    val triggerInterval =
      if (sparkSubConfig.hasPath("trigger-interval-ms")) Some(sparkSubConfig.getLong("trigger-interval-ms")) else None

    val stateless = if (sparkSubConfig.hasPath("nifi-stateless")) {

      Some(
        NifiStatelessConfigModel(
          sparkSubConfig.getString("nifi-stateless.bootstrap"),
          sparkSubConfig.getString("nifi-stateless.system"),
          sparkSubConfig.getString("nifi-stateless.stateless"),
          sparkSubConfig.getString("nifi-stateless.extensions")
        )
      )

    } else {
      None
    }

    SparkStreamingConfigModel(
      appName = sparkSubConfig.getString("app-name"),
      master = readConnectionConfig(sparkSubConfig.getConfig("master")),
      driver = readSparkDriverConf(sparkSubConfig.getConfig("driver-conf")),
      executorCores = sparkSubConfig.getInt("executor-cores"),
      executorMemory = sparkSubConfig.getString("executor-memory"),
      coresMax = sparkSubConfig.getInt("cores-max"),
      executorInstances = sparkSubConfig.getInt("executor-instances"),
      additionalJarsPath = sparkSubConfig.getString("additional-jars-path"),
      yarnJar = sparkSubConfig.getString("yarn-jar"),
      blockManagerPort = sparkSubConfig.getInt("block-manager-port"),
      retained = RetainedConfigModel(
        retainedStagesJobs = sparkSubConfig.getInt("retained-stages-jobs"),
        retainedTasks = sparkSubConfig.getInt("retained-tasks"),
        retainedJobs = sparkSubConfig.getInt("retained-jobs"),
        retainedExecutions = sparkSubConfig.getInt("retained-executions"),
        retainedBatches = sparkSubConfig.getInt("retained-batches")
      ),
      kryoSerializer = readKryoSerializerConfig(sparkSubConfig.getConfig("kryo-serializer")),
      streamingBatchIntervalMs = sparkSubConfig.getInt("streaming-batch-interval-ms"),
      checkpointDir = sparkSubConfig.getString("checkpoint-dir"),
      enableHiveSupport = sparkSubConfig.hasPath("enable-hive-support") && sparkSubConfig.getBoolean("enable-hive-support"),
      triggerIntervalMs = triggerInterval,
      others = readOthersConfig(sparkSubConfig).map(e => SparkEntryConfig(e._1, e._2)),
      nifiStateless = stateless,
      schedulingStrategy = parseSchedulingStrategyConfigModel(sparkSubConfig),
      name = sparkStreamingConfigName
    )
  }

  def parseSchedulingStrategyConfigModel(config: Config): SchedulingStrategyConfigModel = {
    if (config.hasPath("scheduling-strategy")) {
      val innerConfig = config.getConfig("scheduling-strategy")

      if (innerConfig.hasPath("class-name")) {
        SchedulingStrategyConfigModel(innerConfig.getString("class-name"), innerConfig)
      } else {
        throw new Exception("Expected a [scheduling-strategy.class-name] config key")
      }

    } else {
      SchedulingStrategyConfigModel(
        "it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.FifoSchedulingStrategyFactory",
        ConfigFactory.empty()
      )
    }
  }

  def initializeSparkBatchConfig(): Unit = {
    sparkBatchConfig = retrieveConf[SparkBatchConfigModel](getDefaultSparkBatchConfig, sparkBatchConfigName).get
  }

  private def getDefaultSparkBatchConfig: SparkBatchConfigModel = {
    val sparkSubConfig = conf.getConfig("spark-batch")

    SparkBatchConfigModel(
      sparkSubConfig.getString("app-name"),
      readConnectionConfig(sparkSubConfig.getConfig("master")),
      readSparkDriverConf(sparkSubConfig.getConfig("driver-conf")),
      sparkSubConfig.getInt("executor-cores"),
      sparkSubConfig.getString("executor-memory"),
      sparkSubConfig.getInt("cores-max"),
      sparkSubConfig.getInt("executor-instances"),
      sparkSubConfig.getString("additional-jars-path"),
      sparkSubConfig.getString("yarn-jar"),
      sparkSubConfig.getInt("block-manager-port"),
      RetainedConfigModel(
        sparkSubConfig.getInt("retained-stages-jobs"),
        sparkSubConfig.getInt("retained-tasks"),
        sparkSubConfig.getInt("retained-jobs"),
        sparkSubConfig.getInt("retained-executions"),
        sparkSubConfig.getInt("retained-batches")
      ),
      readKryoSerializerConfig(sparkSubConfig.getConfig("kryo-serializer")),
      readOthersConfig(sparkSubConfig).map(e => SparkEntryConfig(e._1, e._2)),
      sparkBatchConfigName
    )
  }

  private def initializeElasticConfig(): Unit = {
    elasticConfig = retrieveConf[ElasticConfigModel](getDefaultElasticConfig, elasticConfigName).get
  }

  private def getDefaultElasticConfig: ElasticConfigModel = {
    val elasticSubConfig = conf.getConfig("elastic")
    ElasticConfigModel(
      readConnectionsConfig(elasticSubConfig, "connections"),
      elasticConfigName
    )
  }

  private def initializeSolrConfig(): Unit = {
    solrConfig = retrieveConf[SolrConfigModel](getDefaultSolrConfig, solrConfigName).get
  }

  private def getDefaultSolrConfig: SolrConfigModel = {
    val solrSubConfig = conf.getConfig("solrcloud")
    SolrConfigModel(
      readZookeeperConnectionsConfig(solrSubConfig),
      solrConfigName
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
      readOthersConfig(hbaseSubConfig).map(e => HBaseEntryConfig(e._1, e._2)),
      hbaseConfigName
    )
  }

  private def initializeJdbcConfig(): Unit = {
    jdbcConfig = retrieveConf[JdbcConfigModel](getDefaultJdbcConfig, jdbcConfigName).get
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

  def initializeNifiConfig(): Unit = {
    nifiConfig = retrieveConf[NifiConfigModel](getDefaultNifiConfig, nifiConfigName).get
  }

  def getDefaultNifiConfig: NifiConfigModel = {
    val nifiSubConfig = conf.getConfig("nifi")
    NifiConfigModel(
      nifiSubConfig.getString("base-url"),
      nifiSubConfig.getString("api-path"),
      nifiSubConfig.getString("ui-path"),
      nifiConfigName
    )
  }

  def getNifiConfig: NifiConfigModel = {
    if (nifiConfig == null) {
      throw new Exception("The nifi configuration was not initialized")
    }
    nifiConfig
  }

  /**
    * Initialize the configurations managed by this ConfigManager.
    *
    * Not initialize WaspDB due to already initialized
    */
  def initializeCommonConfigs(): Unit = {

    if (waspConfig == null) {
      initializeWaspConfig() // already initialized within WaspDB.initializeDB() due to logger.info()
    }

    initializeTelemetryConfig()
    initializeKafkaConfig()
    initializeElasticConfig()
    initializeSolrConfig()
    initializeHBaseConfig()
    initializeJdbcConfig()
    initializeSparkStreamingConfig()
    initializeSparkBatchConfig()
    initializeAvroSchemaManagerConfig()
    initializeNifiConfig()
    initializeCompilerConfig()
  }

  def getWaspConfig: WaspConfigModel = {
    if (waspConfig == null) {
      initializeWaspConfig()
    }
    waspConfig
  }

  def getTelemetryConfig: TelemetryConfigModel = {
    if (telemetryConfig == null) {
      initializeTelemetryConfig()
    }
    telemetryConfig
  }

  def getMongoDBConfig: MongoDBConfigModel = {
    if (mongoDBConfig == null) {
      initializeMongoDBConfig()
    }
    mongoDBConfig
  }

  def getPostgresDBConfig: PostgresDBConfigModel = {
    if (pgDBConfig == null) {
      initializePostgresDBConfig()
    }
    pgDBConfig
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

  def getCompilerConfig: CompilerConfigModel = {
    if (compilerConfig == null) {
      throw new Exception("The spark batch configuration was not initialized")
    }
    compilerConfig
  }

  def getSparkStreamingConfig: SparkStreamingConfigModel = {
    if (sparkStreamingConfig == null) {
      throw new Exception("The spark streaming configuration was not initialized")
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

  def getAvroSchemaManagerConfig: Config = {
    if (avroSchemaManagerConfig == null) {
      throw new Exception("The avroSchemaManagerConfig configuration was not initialized")
    }
    avroSchemaManagerConfig
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

  private def readRestHttpsConfig(): Option[RestHttpsConfigModel] = {
    val httpsActive = conf.hasPath("rest.server.https.active") && conf.getBoolean("rest.server.https.active")

    if (httpsActive) {
      val restHttpsSubConf = conf.getConfig("rest.server.https")
      Some(
        RestHttpsConfigModel(
          keystoreLocation = restHttpsSubConf.getString("keystore-location"),
          passwordLocation = restHttpsSubConf.getString("password-location"),
          keystoreType = restHttpsSubConf.getString("keystore-type")
        )
      )
    } else {
      None
    }

  }

  private def readValidationRulesToIgnore(config: Config, path: String): Array[String] = {
    val validationRulesToIgnore = config.getStringList(path).asScala
    validationRulesToIgnore.toArray
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
      config.getInt("driver-port"),
      config.getBoolean("kill-driver-process-if-spark-context-stops")
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
    * Read the configuration with the specified name from MongoDB or,
    * if it is not present, initialize it with the provided defaults.
    */
  private def retrieveConf[T <: Model : CanOverrideName](
                                                          default: T,
                                                          nameConf: String
                                                        )(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[T] = {

    namespaced(default, nameConf) match {
      case (entity, Some(name)) =>
        ConfigBL.configManagerBL.retrieveConf(entity, name)(ct, typeTag)
      case (entity, None) =>
        Some(entity)
    }

  }

  def readOthersConfig(config: Config): Seq[(String, String)] = {
    val key = "others"

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

trait NifiConfiguration {
  lazy val nifiConfig: NifiConfigModel = ConfigManager.getNifiConfig
}
