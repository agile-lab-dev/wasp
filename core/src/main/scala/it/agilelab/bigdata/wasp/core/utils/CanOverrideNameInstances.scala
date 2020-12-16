package it.agilelab.bigdata.wasp.core.utils

import it.agilelab.bigdata.wasp.models.configuration._

trait CanOverrideNameInstances {
  implicit val telemetryConfigModelCanOverrideName: CanOverrideName[TelemetryConfigModel] =
    CanOverrideName((instance, newName) => instance.copy(name = newName))
  implicit val kafkaConfigModelCanOverrideName: CanOverrideName[KafkaConfigModel] =
    CanOverrideName((instance, newName) => instance.copy(name = newName))
  implicit val sparkStreamingConfigModelCanOverrideName: CanOverrideName[SparkStreamingConfigModel] =
    CanOverrideName((instance, newName) => instance.copy(name = newName))
  implicit val sparkBatchConfigModelCanOverrideName: CanOverrideName[SparkBatchConfigModel] =
    CanOverrideName((instance, newName) => instance.copy(name = newName))
  implicit val solrConfigModelCanOverrideName: CanOverrideName[SolrConfigModel] =
    CanOverrideName((instance, newName) => instance.copy(name = newName))
  implicit val jdbcConfigModelCanOverrideName: CanOverrideName[JdbcConfigModel] =
    CanOverrideName((instance, newName) => instance.copy(name = newName))
  implicit val compilerConfigModelCanOverrideName: CanOverrideName[CompilerConfigModel] =
    CanOverrideName((instance, newName) => instance.copy(name = newName))
  implicit val elasticConfigModelCanOverrideName: CanOverrideName[ElasticConfigModel] =
    CanOverrideName((instance, newName) => instance.copy(name = newName))
  implicit val nifiConfigModelCanOverrideName: CanOverrideName[NifiConfigModel] =
    CanOverrideName((instance, newName) => instance.copy(name = newName))
  implicit val hbaseConfigModelCanOverrideName: CanOverrideName[HBaseConfigModel] =
    CanOverrideName((instance, newName) => instance.copy(name = newName))
}
