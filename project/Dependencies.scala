import sbt._

/*
 * Dependencies definitions. Keep in alphabetical order.
 *
 * See project/Versions.scala for the versions definitions.
 */
object Dependencies {

	implicit class Exclude(module: ModuleID) {
		def log4jExclude: ModuleID =
			module excludeAll ExclusionRule("log4j")

		def embeddedExclusions: ModuleID =
			module.log4jExclude
				.excludeAll(ExclusionRule("org.apache.spark"))
				.excludeAll(ExclusionRule("com.typesafe"))

		def driverExclusions: ModuleID =
			module.log4jExclude
				.exclude("com.google.guava", "guava")
				.excludeAll(ExclusionRule("org.slf4j"))

		def hbaseExclusion: ModuleID =
			module.log4jExclude
				.exclude("io.netty", "netty")
				.exclude("io.netty", "netty-all")
				.exclude("com.google.guava", "guava")
				.exclude("org.mortbay.jetty", "jetty-util")
				.exclude("org.mortbay.jetty", "jetty")
				.exclude("org.apache.zookeeper", "zookeeper")

		def solrExclusion: ModuleID =
			module.log4jExclude
				.exclude("org.objenesis", "objenesis")
				.exclude("org.apache.zookeeper", "zookeeper")

		def sparkExclusions: ModuleID =
			module.log4jExclude
				.exclude("io.netty", "netty")
				.exclude("com.google.guava", "guava")
				.exclude("org.apache.spark", "spark-core")
				.exclude("org.apache.kafka", "kafka-clients")
				.exclude("org.slf4j", "slf4j-log4j12")
				.exclude("org.apache.logging.log4j", "log4j-api")
				.exclude("org.apache.logging.log4j", "log4j-core")
				.exclude("org.apache.logging.log4j", "log4j-slf4j-impl")
	  		.exclude("org.apache.solr", "solr-solrj")
				.exclude("org.apache.solr", "solr-core")
				.exclude("org.apache.solr", "solr-test-framework")

		def kafkaExclusions: ModuleID =
			module
				.excludeAll(ExclusionRule("org.slf4j"))
				.exclude("com.sun.jmx", "jmxri")
				.exclude("com.sun.jdmk", "jmxtools")
				.exclude("net.sf.jopt-simple", "jopt-simple")

		def kafka8Exclusions: ModuleID =
			module
				.exclude("org.apache.kafka", "kafka_2.11")

		def camelKafkaExclusions: ModuleID =
			module
				.exclude("org.apache.kafka", "kafka-clients")
	}
	
	val excludeLog4j: (sbt.ModuleID) => ModuleID = (module: ModuleID) =>
		module.excludeAll(
			sbt.ExclusionRule(organization = "org.apache.logging.log4j", name = "log4j-api"),
			sbt.ExclusionRule(organization = "org.apache.logging.log4j", name = "log4j-core"),
			sbt.ExclusionRule(organization = "org.apache.logging.log4j", name = "log4j-slf4j-impl"),
			sbt.ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12")
		)

	val excludeNetty: (sbt.ModuleID) => ModuleID = (module: ModuleID) =>
		module.excludeAll(
			sbt.ExclusionRule(organization = "io.netty", name = "netty"),
			sbt.ExclusionRule(organization = "io.netty", name = "netty-all")
			//sbt.ExclusionRule(organization = "com.google.guava", name = "guava")
		)
	// ===================================================================================================================
	// Compile dependencies
	// ===================================================================================================================
	val akkaActor = "com.typesafe.akka" %% "akka-actor" % Versions.akka
	val akkaCamel = "com.typesafe.akka" %% "akka-camel" % Versions.akka
	val akkaCluster = "com.typesafe.akka" %% "akka-cluster" % Versions.akka
	val akkaClusterTools = "com.typesafe.akka" %% "akka-cluster-tools" % Versions.akka
	val akkaContrib = "com.typesafe.akka" %% "akka-contrib" % Versions.akka
	val akkaHttp = "com.typesafe.akka" %% "akka-http" % Versions.akkaHttp
	val akkaHttpSpray = "com.typesafe.akka" %% "akka-http-spray-json" % Versions.akkaHttp
	val akkaRemote = "com.typesafe.akka" %% "akka-remote" % Versions.akka
	val akkaSlf4j = "com.typesafe.akka" %% "akka-slf4j" % Versions.akka
	val akkaStream = "com.typesafe.akka" %% "akka-stream" % Versions.akka
	val apacheCommonsLang3 = "org.apache.commons" % "commons-lang3" % Versions.apacheCommonsLang3Version // remove?
	val avro = "org.apache.avro" % "avro" % Versions.avro
	val camelKafka = ("org.apache.camel" % "camel-kafka" % Versions.camel).kafkaExclusions.camelKafkaExclusions
	val camelWebsocket = "org.apache.camel" % "camel-websocket" % Versions.camel
	val commonsCli = "commons-cli" % "commons-cli" % Versions.commonsCli
  val elasticSearch = "org.elasticsearch" % "elasticsearch" % Versions.elasticSearch
  val elasticSearchSpark = "org.elasticsearch" %% "elasticsearch-spark-20" % Versions.elasticSearchSpark
	val guava = "com.google.guava" % "guava" % Versions.guava
	val hbaseClient = "org.apache.hbase" % "hbase-client" % Versions.hbase hbaseExclusion
	val hbaseCommon = "org.apache.hbase" % "hbase-common" % Versions.hbase hbaseExclusion
	val hbaseServer = "org.apache.hbase" % "hbase-server" % Versions.hbase hbaseExclusion
	val httpmime = "org.apache.httpcomponents" % "httpmime" % "4.3.1" // TODO remove?
	val jodaConvert = "org.joda" % "joda-convert" % Versions.jodaConvert
	val jodaTime = "joda-time" % "joda-time" % Versions.jodaTime
	val json4sCore = "org.json4s" %% "json4s-core" % Versions.json4s
	val json4sJackson = "org.json4s" %% "json4s-jackson" % Versions.json4s
	val json4sNative = "org.json4s" %% "json4s-native" % Versions.json4s
  val kafka = "org.apache.kafka" %% "kafka" % Versions.cdk kafkaExclusions
  val kafkaClients = "org.apache.kafka" % "kafka-clients" % Versions.cdk kafkaExclusions
  val kafkaStreaming = ("org.apache.spark" %% "spark-streaming-kafka-0-8" % Versions.spark).sparkExclusions.kafka8Exclusions
  val kafkaSparkSql = "org.apache.spark" %% "spark-sql-kafka-0-10" % Versions.spark sparkExclusions
	val log4jApi = "org.apache.logging.log4j" % "log4j-api" % Versions.log4j % "optional,test"
	val log4jCore = "org.apache.logging.log4j" % "log4j-core" % Versions.log4j % "optional,test"
	val log4jSlf4jImpl = "org.apache.logging.log4j" % "log4j-slf4j-impl" % Versions.log4j % "optional,test"
	val metrics = "com.yammer.metrics" % "metrics-core" % "2.2.0" // TODO upgrade?
	val mongodbScala = "org.mongodb.scala" %% "mongo-scala-driver" % Versions.mongodbScala
	val netty = "io.netty" % "netty" % Versions.netty
	val nettySpark = "io.netty" % "netty" % Versions.nettySpark
	val nettyAll = "io.netty" % "netty-all" % Versions.nettyAllSpark
	val quartz = "org.quartz-scheduler" % "quartz" % Versions.quartz
	val scalaj = "org.scalaj" %% "scalaj-http" % "1.1.4" // TODO remove?
	val scaldi = "org.scaldi" %% "scaldi-akka" % "0.3.3" // TODO remove?
	val slf4jApi = "org.slf4j" % "slf4j-api" % Versions.slf4j
	val solr = "org.apache.solr" % "solr-solrj" % Versions.solr solrExclusion
	val solrCore = "org.apache.solr" % "solr-core" % Versions.solr solrExclusion
  val sparkSolr = ("it.agilelab.bigdata.spark" % "spark-solr" % Versions.solrSpark).sparkExclusions.solrExclusion /*classifier "spark-2.2-scala-2.11"*/
  val sparkCore = "org.apache.spark" %% "spark-core" % Versions.spark sparkExclusions
  val sparkMLlib = "org.apache.spark" %% "spark-mllib" % Versions.spark sparkExclusions
  val sparkSQL = "org.apache.spark" %% "spark-sql" % Versions.spark sparkExclusions
  val sparkYarn = "org.apache.spark" %% "spark-yarn" % Versions.spark sparkExclusions
	val typesafeConfig = "com.typesafe" % "config" % "1.3.0"
	val zkclient = "com.101tec" % "zkclient" % "0.3"


	// grouped dependencies, for convenience =============================================================================
	val akka = Seq(
		akkaActor,
		akkaCluster,
		akkaClusterTools,
		akkaContrib,
		akkaRemote,
		akkaSlf4j
	)

	val hbase = Seq(hbaseClient, hbaseCommon, hbaseServer)

	val json = Seq(json4sCore, json4sJackson, json4sNative)

	val logging = Seq(slf4jApi)

	val log4j = Seq(log4jApi, log4jCore, log4jSlf4jImpl)

	val spark = Seq(sparkCore, sparkMLlib, sparkSQL, sparkYarn)

	val time = Seq(jodaConvert, jodaTime)


	// ===================================================================================================================
	// Test dependencies
	// ===================================================================================================================
	val akkaClusterTestKit = "com.typesafe.akka" %% "akka-multi-node-testkit" % Versions.akka % Test
	val akkaTestKit = "com.typesafe.akka" %% "akka-testkit" % Versions.akka % Test
	val scalatest = "org.scalatest" %% "scalatest" % Versions.scalaTest % Test

	// grouped dependencies, for convenience =============================================================================
  val test = Seq(akkaTestKit, akkaClusterTestKit, scalatest)

	// ===================================================================================================================
	// Module dependencies
	// ===================================================================================================================
	val core = (akka ++
		logging ++
		time ++
		json ++
		test :+
		akkaHttp :+
		akkaHttpSpray :+
		avro :+
		commonsCli :+
		kafka :+ // TODO remove when switching to plugins
		mongodbScala :+
		sparkSQL :+
    typesafeConfig
	).map(excludeLog4j).map(excludeNetty)

	val producers = (
		akka ++
		test :+
		akkaHttp :+
		akkaStream :+
		netty
	).map(excludeLog4j) ++ log4j

	val consumers_spark = (
		akka ++
		json ++
		test ++
		spark :+
    quartz
	).map(excludeNetty).map(excludeLog4j) ++
		log4j :+
		nettySpark :+
		nettyAll :+
		guava

	val consumers_rt = (
		akka :+
		akkaCamel :+
		camelKafka :+
		camelWebsocket :+
		kafka  :+
		netty
	).map(excludeLog4j) ++ log4j

  val master = (
	  akka :+
		akkaHttp :+
		akkaHttpSpray :+
		netty
  ).map(excludeLog4j) ++ log4j

	val plugin_elastic_spark = Seq(
		elasticSearchSpark
	)
	
  val plugin_hbase_spark = (
		hbase :+
		scalatest
	).map(excludeNetty)
	
	val plugin_kafka_spark = Seq(
		kafkaStreaming,
		kafkaSparkSql
	).map(excludeLog4j).map(excludeNetty)
	
	val plugin_solr_spark = Seq(
		solr,
		sparkSolr
	).map(excludeNetty)
}