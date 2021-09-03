import sbt._

/*
 * Dependencies definitions. Keep in alphabetical order.
 *
 * See project/Versions.scala for the versions definitions.
 */
trait Dependencies {

  val scalaCompilerDependencies: Seq[ModuleID]
  val modelDependencies: Seq[ModuleID]
  val coreDependencies: Seq[ModuleID]
  val testDependencies: Seq[ModuleID]
  val scalaTestDependencies: Seq[ModuleID]
  val repositoryCoreDependencies: Seq[ModuleID]
  val repositoryMongoDependencies: Seq[ModuleID]
  val repositoryPostgresDependencies: Seq[ModuleID]
  val masterDependencies: Seq[ModuleID]
  val producersDependencies: Seq[ModuleID]
  val consumersSparkDependencies: Seq[ModuleID]
  val consumersRtDependencies: Seq[ModuleID]
  val pluginElasticSparkDependencies: Seq[ModuleID]
  val pluginHbaseSparkDependencies: Seq[ModuleID]
  val pluginKafkaSparkDependencies: Seq[ModuleID]
  val pluginKafkaSparkNewDependencies: Seq[ModuleID]
  val pluginKafkaSparkOldDependencies: Seq[ModuleID]
  val pluginSolrSparkDependencies: Seq[ModuleID]
  val pluginMongoSparkDependencies: Seq[ModuleID]
  val pluginMailerSparkDependencies: Seq[ModuleID]
  val pluginHttpSparkDependencies: Seq[ModuleID]
  val pluginCdcSparkDependencies: Seq[ModuleID]
  val microserviceCatalogDependencies: Seq[ModuleID]
  val pluginParallelWriteSparkDependencies: Seq[ModuleID]
  val yarnAuthHdfsDependencies: Seq[ModuleID]
  val yarnAuthHBaseDependencies: Seq[ModuleID]
  val sparkTelemetryPluginDependencies: Seq[ModuleID]
  val sparkNifiPluginDependencies: Seq[ModuleID]
  val nifiStatelessDependencies: Seq[ModuleID]
  val nifiClientDependencies: Seq[ModuleID]
  val whitelabelModelsDependencies: Seq[ModuleID]
  val whitelabelMasterDependencies: Seq[ModuleID]
  val whitelabelProducerDependencies: Seq[ModuleID]
  val whitelabelSparkConsumerDependencies: Seq[ModuleID]
  val whiteLabelConsumersRtDependencies: Seq[ModuleID]
  val openapiDependencies: Seq[ModuleID]
  val kmsTest: Seq[Def.Setting[_]]
  val sparkPluginBasicDependencies: Seq[ModuleID]

  val whitelabelMasterScriptClasspath: Def.Setting[Task[Seq[String]]]
  val whitelabelProducerScriptClasspath: Def.Setting[Task[Seq[String]]]
  val whitelabelSparkConsumerScriptClasspath: Def.Setting[Task[Seq[String]]]
  val whiteLabelConsumersRtScriptClasspath: Def.Setting[Task[Seq[String]]]
  val whiteLabelSingleNodeScriptClasspath: Def.Setting[Task[Seq[String]]]
}
