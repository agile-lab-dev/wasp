import sbt._

/*
 * Dependencies definitions.
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
		
		def sparkExclusions: ModuleID =
			module.log4jExclude
				.exclude("com.google.guava", "guava")
				.exclude("org.apache.spark", "spark-core")
				.exclude("org.slf4j", "slf4j-log4j12")
		
		def kafkaExclusions: ModuleID =
			module
				.excludeAll(ExclusionRule("org.slf4j"))
				.exclude("com.sun.jmx", "jmxri")
				.exclude("com.sun.jdmk", "jmxtools")
				.exclude("net.sf.jopt-simple", "jopt-simple")
		
		def sparkSolrExclusions: ModuleID =
			module.excludeAll(
				ExclusionRule(organization = "org.eclipse.jetty"),
				ExclusionRule(organization = "javax.servlet"),
				ExclusionRule(organization = "org.eclipse.jetty.orbit")
			)
	}
	
	// ===================================================================================================================
	// Compile dependencies
	// ===================================================================================================================
	val playcore = "com.typesafe.play" %% "play" % Versions.play
	val playws = "com.typesafe.play" %% "play-ws" % Versions.play
	val playcache = "com.typesafe.play" %% "play-cache" % Versions.play
	val playjson = "com.typesafe.play" %% "play-json" % Versions.play
	val playserver = "com.typesafe.play" %% "play-netty-server" % Versions.play
	val akkaStream = "com.typesafe.akka" %% "akka-stream-experimental" % Versions.akkaStreams
	val akkaHttpCore = "com.typesafe.akka" %% "akka-http-core-experimental" % Versions.akkaStreams
	val akkaActor = "com.typesafe.akka" %% "akka-actor" % Versions.akka
	val akkaCluster = "com.typesafe.akka" %% "akka-cluster" % Versions.akka
	val akkaContrib = "com.typesafe.akka" %% "akka-contrib" % Versions.akka
	val akkaRemote = "com.typesafe.akka" %% "akka-remote" % Versions.akka
	val akkaSlf4j = "com.typesafe.akka" %% "akka-slf4j" % Versions.akka
	val akkaCamel = "com.typesafe.akka" %% "akka-camel" % Versions.akka
	val avro = "org.apache.avro" % "avro" % Versions.avro
	
	val elasticSearch = "org.elasticsearch" % "elasticsearch" % Versions.elasticSearch
	val elasticSearchSpark = "org.elasticsearch" %% "elasticsearch-spark" % Versions.elasticSearchSpark
	val jodaTime = "joda-time" % "joda-time" % Versions.jodaTime % "compile;runtime"
	// ApacheV2
	val jodaConvert = "org.joda" % "joda-convert" % Versions.jodaConvert % "compile;runtime"
	// ApacheV2
	val json4sCore = "org.json4s" %% "json4s-core" % Versions.json4s
	// ApacheV2
	val json4sJackson = "org.json4s" %% "json4s-jackson" % Versions.json4s
	// ApacheV2
	val json4sNative = "org.json4s" %% "json4s-native" % Versions.json4s
	// ApacheV2
	val kafka = "org.apache.kafka" %% "kafka" % Versions.kafka kafkaExclusions
	// ApacheV2
	val kafkaClients =
		"org.apache.kafka" % "kafka-clients" % Versions.kafka kafkaExclusions
	// ApacheV2
	val kafkaStreaming =
		"org.apache.spark" %% "spark-streaming-kafka" % Versions.spark sparkExclusions
	// ApacheV2
	//val logback = "ch.qos.logback" % "logback-classic" % Logback
	//val logbackCore = "ch.qos.logback" % "logback-core" % Logback
	val reactive_mongo = "org.reactivemongo" %% "reactivemongo" % Versions.reactiveMongo
	// MIT
	val slf4jApi = "org.slf4j" % "slf4j-api" % Versions.slf4j
	// MIT
	val sparkML = "org.apache.spark" %% "spark-mllib" % Versions.spark sparkExclusions
	// ApacheV2
	val sparkCatalyst =
		"org.apache.spark" %% "spark-catalyst" % Versions.spark sparkExclusions
	// ApacheV2
	val sparkYarn = "org.apache.spark" %% "spark-yarn" % Versions.spark sparkExclusions
	
	// TODO verify license
	val solr = "org.apache.solr" % "solr-solrj" % Versions.solr
	val solrspark =
		"it.agilelab.bigdata.spark" % "spark-solr" % Versions.solrSpark sparkSolrExclusions
	
	val hbasespark = "org.apache.hbase" % "hbase-spark" % Versions.hbaseSpark
	val hbasecommond = "org.apache.hbase" % "hbase-common" % Versions.hbaseSpark
	val hbaseclient = "org.apache.hbase" % "hbase-client" % Versions.hbaseSpark
	val hbaseserver = "org.apache.hbase" % "hbase-server" % Versions.hbaseSpark
	
	val asynchttpclient = "com.ning" % "async-http-client" % "1.9.39"
	
	val scaldi = "org.scaldi" %% "scaldi-akka" % "0.3.3"
	val apacheCommonsLang3 = "org.apache.commons" % "commons-lang3" % Versions.apacheCommonsLang3Version
	val camelKafka = "org.apache.camel" % "camel-kafka" % Versions.camelKafka
	val camelWebsocket = "org.apache.camel" % "camel-websocket" % Versions.camelWebsocket
	val camelElastic = "org.apache.camel" % "camel-elasticsearch" % Versions.camelElasticSearch
	val camelQuartz2 = "org.apache.camel" % "camel-quartz2" % Versions.camelQuartz2
	val zkclient = "com.101tec" % "zkclient" % "0.3"
	
	val typesafeConfig = "com.typesafe" % "config" % "1.3.0"
	
	val scalaj = "org.scalaj" %% "scalaj-http" % "1.1.4"
	
	val metrics = "com.yammer.metrics" % "metrics-core" % "2.2.0"
	
	val httpmime = "org.apache.httpcomponents" % "httpmime" % "4.3.1"
	
	val log4jApi = "org.apache.logging.log4j" % "log4j-api" % "2.8.2"
	val log4jCore = "org.apache.logging.log4j" % "log4j-core" % "2.8.2"
	val log4jSl4jImpl = "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.8.2"
	
	// grouped dependencies, for convenience =============================================================================
	val play = Seq(playcore, playws, playcache, playjson, playserver)
	
	val akka = Seq(akkaStream,
	               akkaHttpCore,
	               akkaActor,
	               akkaCluster,
	               akkaContrib,
	               akkaRemote,
	               akkaSlf4j,
	               akkaCamel)
	
	val spark = Seq(sparkCatalyst)
	
	val apachesolr = Seq(solr, solrspark)
	
	val json = Seq(json4sCore, json4sJackson, json4sNative)
	
	val logging = Seq(slf4jApi, log4jApi, log4jCore, log4jSl4jImpl)
	
	val time = Seq(jodaConvert, jodaTime)
	
	val elastic = Seq(elasticSearch)
	
	
	// ===================================================================================================================
	// Test dependencies
	// ===================================================================================================================
	val akkaTestKit = "com.typesafe.akka" %% "akka-testkit" % Versions.akka % "test" // ApacheV2
	val akkaClusterTest = "com.typesafe.akka" %% "akka-multi-node-testkit" % Versions.akka % "test"
	val scalatest = "org.scalatest" %% "scalatest" % Versions.scalaTest % "test"
	
	// grouped dependencies, for convenience =============================================================================
	val test = Seq(akkaTestKit, akkaClusterTest, scalatest)
	
	// ===================================================================================================================
	// Module dependencies
	// ===================================================================================================================
	val core = akka ++ logging ++ time ++ test ++ elastic ++ apachesolr ++
		Seq(avro,
		    reactive_mongo,
		    kafka,
		    playjson,
		    playws,
		    scaldi,
		    apacheCommonsLang3,
		    sparkYarn,
		    asynchttpclient,
		    typesafeConfig)
	
	val master = play ++ logging ++ time ++ json ++ elastic
	
	val wasp_producers = akka ++ logging ++ test ++
		Seq(playws)
	
	val wasp_consumers = json ++ test ++
		Seq(kafka,
		    kafkaStreaming,
		    sparkML,
		    sparkCatalyst,
		    elasticSearchSpark,
		    camelKafka,
		    camelWebsocket,
		    camelElastic,
		    camelQuartz2,
		    hbasespark,
		    hbaseclient,
		    hbaseserver,
		    hbasecommond)
}