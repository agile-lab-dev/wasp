package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.configuration.{
  CompilerConfigModel,
  ElasticConfigModel,
  JdbcConfigModel,
  KafkaConfigModel,
  NifiConfigModel,
  SolrConfigModel,
  SparkBatchConfigModel,
  SparkStreamingConfigModel,
  TelemetryConfigModel
}
import it.agilelab.bigdata.wasp.repository.core.dbModels.{
  CompilerConfigDBModel,
  CompilerConfigDBModelV1,
  ElasticConfigDBModel,
  ElasticConfigDBModelV1,
  JdbcConfigDBModel,
  JdbcConfigDBModelV1,
  KafkaConfigDBModel,
  KafkaConfigDBModelV1,
  NifiConfigDBModel,
  NifiConfigDBModelV1,
  SolrConfigDBModel,
  SolrConfigDBModelV1,
  SparkBatchConfigDBModel,
  SparkBatchConfigDBModelV1,
  SparkStreamingConfigDBModel,
  SparkStreamingConfigDBModelV1,
  TelemetryConfigDBModel,
  TelemetryConfigDBModelV1
}

object SolrConfigMapperSelector extends MapperSelector[SolrConfigModel, SolrConfigDBModel]

object SolrConfigMapperV1 extends SimpleMapper[SolrConfigModel, SolrConfigDBModelV1] {
  override val version = "solrConfigV1"
  override def fromDBModelToModel[B >: SolrConfigDBModelV1](m: B): SolrConfigModel = m match {
    case mm: SolrConfigDBModelV1 => transform[SolrConfigModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }
}

object KafkaConfigMapperSelector extends MapperSelector[KafkaConfigModel, KafkaConfigDBModel]

object KafkaConfigMapperV1 extends SimpleMapper[KafkaConfigModel, KafkaConfigDBModelV1] {
  override val version = "kafkaConfigV1"
  override def fromDBModelToModel[B >: KafkaConfigDBModelV1](m: B): KafkaConfigModel = m match {
    case mm: KafkaConfigDBModelV1 => transform[KafkaConfigModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }
}

object SparkBatchConfigMapperSelector extends MapperSelector[SparkBatchConfigModel, SparkBatchConfigDBModel]

object SparkBatchConfigMapperV1 extends SimpleMapper[SparkBatchConfigModel, SparkBatchConfigDBModelV1] {
  override val version = "sparkBatchConfigV1"
  override def fromDBModelToModel[B >: SparkBatchConfigDBModelV1](m: B): SparkBatchConfigModel = m match {
    case mm: SparkBatchConfigDBModelV1 => transform[SparkBatchConfigModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }
}

object SparkStreamingConfigMapperSelector extends MapperSelector[SparkStreamingConfigModel, SparkStreamingConfigDBModel]

object SparkStreamingConfigMapperV1 extends SimpleMapper[SparkStreamingConfigModel, SparkStreamingConfigDBModelV1] {
  override val version = "sparkStreamingConfigV1"
  override def fromDBModelToModel[B >: SparkStreamingConfigDBModelV1](m: B): SparkStreamingConfigModel = m match {
    case mm: SparkStreamingConfigDBModelV1 => transform[SparkStreamingConfigModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }
}

object ElasticConfigMapperSelector extends MapperSelector[ElasticConfigModel, ElasticConfigDBModel]

object ElasticConfigMapperV1 extends SimpleMapper[ElasticConfigModel, ElasticConfigDBModelV1] {
  override val version = "elasticConfigV1"
  override def fromDBModelToModel[B >: ElasticConfigDBModelV1](m: B): ElasticConfigModel = m match {
    case mm: ElasticConfigDBModelV1 => transform[ElasticConfigModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }
}

object JdbcConfigMapperSelector extends MapperSelector[JdbcConfigModel, JdbcConfigDBModel]

object JdbcConfigMapperV1 extends SimpleMapper[JdbcConfigModel, JdbcConfigDBModelV1] {
  override val version = "jdbcConfigV1"
  override def fromDBModelToModel[B >: JdbcConfigDBModelV1](m: B): JdbcConfigModel = m match {
    case mm: JdbcConfigDBModelV1 => transform[JdbcConfigModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }
}

object TelemetryConfigMapperSelector extends MapperSelector[TelemetryConfigModel, TelemetryConfigDBModel]

object TelemetryConfigMapperV1 extends SimpleMapper[TelemetryConfigModel, TelemetryConfigDBModelV1] {
  override val version = "telemetryConfigV1"
  override def fromDBModelToModel[B >: TelemetryConfigDBModelV1](m: B): TelemetryConfigModel = m match {
    case mm: TelemetryConfigDBModelV1 => transform[TelemetryConfigModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }
}

object NifiConfigMapperSelector extends MapperSelector[NifiConfigModel, NifiConfigDBModel]

object NifiConfigMapperV1 extends SimpleMapper[NifiConfigModel, NifiConfigDBModelV1] {
  override val version = "nifiConfigV1"
  override def fromDBModelToModel[B >: NifiConfigDBModelV1](m: B): NifiConfigModel = m match {
    case mm: NifiConfigDBModelV1 => transform[NifiConfigModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }
}

object CompilerConfigMapperSelector extends MapperSelector[CompilerConfigModel, CompilerConfigDBModel]

object CompilerConfigMapperV1 extends SimpleMapper[CompilerConfigModel, CompilerConfigDBModelV1] {
  override val version = "nifiConfigV1"
  override def fromDBModelToModel[B >: CompilerConfigDBModelV1](m: B): CompilerConfigModel = m match {
    case mm: CompilerConfigDBModelV1 => transform[CompilerConfigModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }
}
