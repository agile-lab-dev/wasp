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

object SolrConfigMapperSelector extends MapperSelector[SolrConfigModel, SolrConfigDBModel] {
  override def select(model: SolrConfigDBModel): Mapper[SolrConfigModel, SolrConfigDBModel] = {

    model match {
      case _: SolrConfigDBModelV1 => SolrConfigMapperV1
      case o                      => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
    }
  }

  def applyMap(p: SolrConfigDBModel): SolrConfigModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object SolrConfigMapperV1 extends Mapper[SolrConfigModel, SolrConfigDBModelV1] {
  override val version = "solrConfigV1"

  override def fromModelToDBModel(p: SolrConfigModel): SolrConfigDBModelV1 = {

    val values      = SolrConfigModel.unapply(p).get
    val makeDBModel = (SolrConfigDBModelV1.apply _).tupled
    makeDBModel(values)
  }

  override def fromDBModelToModel[B >: SolrConfigDBModelV1](p: B): SolrConfigModel = {

    val values       = SolrConfigDBModelV1.unapply(p.asInstanceOf[SolrConfigDBModelV1]).get
    val makeProducer = (SolrConfigModel.apply _).tupled
    makeProducer(values)
  }
}

object KafkaConfigMapperSelector extends MapperSelector[KafkaConfigModel, KafkaConfigDBModel] {
  override def select(model: KafkaConfigDBModel): Mapper[KafkaConfigModel, KafkaConfigDBModel] = {

    model match {
      case _: KafkaConfigDBModelV1 => KafkaConfigMapperV1
      case o                       => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
    }
  }

  def applyMap(p: KafkaConfigDBModel): KafkaConfigModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object KafkaConfigMapperV1 extends Mapper[KafkaConfigModel, KafkaConfigDBModelV1] {
  override val version = "kafkaConfigV1"

  override def fromModelToDBModel(p: KafkaConfigModel): KafkaConfigDBModelV1 = {

    val values      = KafkaConfigModel.unapply(p).get
    val makeDBModel = (KafkaConfigDBModelV1.apply _).tupled
    makeDBModel(values)
  }

  override def fromDBModelToModel[B >: KafkaConfigDBModelV1](p: B): KafkaConfigModel = {

    val values       = KafkaConfigDBModelV1.unapply(p.asInstanceOf[KafkaConfigDBModelV1]).get
    val makeProducer = (KafkaConfigModel.apply _).tupled
    makeProducer(values)
  }
}

object SparkBatchConfigMapperSelector extends MapperSelector[SparkBatchConfigModel, SparkBatchConfigDBModel] {
  override def select(model: SparkBatchConfigDBModel): Mapper[SparkBatchConfigModel, SparkBatchConfigDBModel] = {

    model match {
      case _: SparkBatchConfigDBModelV1 => SparkBatchConfigMapperV1
      case other                        => throw new Exception(s"There is no available mapper for this DBModel: $other, create one!")
    }
  }

  def applyMap(p: SparkBatchConfigDBModel): SparkBatchConfigModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object SparkBatchConfigMapperV1 extends Mapper[SparkBatchConfigModel, SparkBatchConfigDBModelV1] {
  override val version = "sparkBatchConfigV1"

  override def fromModelToDBModel(p: SparkBatchConfigModel): SparkBatchConfigDBModelV1 = {

    val values      = SparkBatchConfigModel.unapply(p).get
    val makeDBModel = (SparkBatchConfigDBModelV1.apply _).tupled
    makeDBModel(values)
  }

  override def fromDBModelToModel[B >: SparkBatchConfigDBModelV1](p: B): SparkBatchConfigModel = {

    val values       = SparkBatchConfigDBModelV1.unapply(p.asInstanceOf[SparkBatchConfigDBModelV1]).get
    val makeProducer = (SparkBatchConfigModel.apply _).tupled
    makeProducer(values)
  }
}

object SparkStreamingConfigMapperSelector
    extends MapperSelector[SparkStreamingConfigModel, SparkStreamingConfigDBModel] {
  override def select(
      model: SparkStreamingConfigDBModel
  ): Mapper[SparkStreamingConfigModel, SparkStreamingConfigDBModel] = {

    model match {
      case _: SparkStreamingConfigDBModelV1 => SparkStreamingConfigMapperV1
      case o                                => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")

    }
  }

  def applyMap(p: SparkStreamingConfigDBModel): SparkStreamingConfigModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object SparkStreamingConfigMapperV1 extends Mapper[SparkStreamingConfigModel, SparkStreamingConfigDBModelV1] {
  override val version = "sparkStreamingConfigV1"

  override def fromModelToDBModel(p: SparkStreamingConfigModel): SparkStreamingConfigDBModelV1 = {

    val values      = SparkStreamingConfigModel.unapply(p).get
    val makeDBModel = (SparkStreamingConfigDBModelV1.apply _).tupled
    makeDBModel(values)
  }

  override def fromDBModelToModel[B >: SparkStreamingConfigDBModelV1](p: B): SparkStreamingConfigModel = {

    val values       = SparkStreamingConfigDBModelV1.unapply(p.asInstanceOf[SparkStreamingConfigDBModelV1]).get
    val makeProducer = (SparkStreamingConfigModel.apply _).tupled
    makeProducer(values)
  }
}

object ElasticConfigMapperSelector extends MapperSelector[ElasticConfigModel, ElasticConfigDBModel] {
  override def select(model: ElasticConfigDBModel): Mapper[ElasticConfigModel, ElasticConfigDBModel] = {

    model match {
      case _: ElasticConfigDBModelV1 => ElasticConfigMapperV1
      case o                         => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")

    }
  }

  def applyMap(p: ElasticConfigDBModel): ElasticConfigModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object ElasticConfigMapperV1 extends Mapper[ElasticConfigModel, ElasticConfigDBModelV1] {
  override val version = "elasticConfigV1"

  override def fromModelToDBModel(p: ElasticConfigModel): ElasticConfigDBModelV1 = {

    val values      = ElasticConfigModel.unapply(p).get
    val makeDBModel = (ElasticConfigDBModelV1.apply _).tupled
    makeDBModel(values)
  }

  override def fromDBModelToModel[B >: ElasticConfigDBModelV1](p: B): ElasticConfigModel = {

    val values       = ElasticConfigDBModelV1.unapply(p.asInstanceOf[ElasticConfigDBModelV1]).get
    val makeProducer = (ElasticConfigModel.apply _).tupled
    makeProducer(values)
  }
}

object JdbcConfigMapperSelector extends MapperSelector[JdbcConfigModel, JdbcConfigDBModel] {
  override def select(model: JdbcConfigDBModel): Mapper[JdbcConfigModel, JdbcConfigDBModel] = {

    model match {
      case _: JdbcConfigDBModelV1 => JdbcConfigMapperV1
      case o                      => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")

    }
  }

  def applyMap(p: JdbcConfigDBModel): JdbcConfigModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object JdbcConfigMapperV1 extends Mapper[JdbcConfigModel, JdbcConfigDBModelV1] {
  override val version = "jdbcConfigV1"

  override def fromModelToDBModel(p: JdbcConfigModel): JdbcConfigDBModelV1 = {

    val values      = JdbcConfigModel.unapply(p).get
    val makeDBModel = (JdbcConfigDBModelV1.apply _).tupled
    makeDBModel(values)
  }

  override def fromDBModelToModel[B >: JdbcConfigDBModelV1](p: B): JdbcConfigModel = {

    val values       = JdbcConfigDBModelV1.unapply(p.asInstanceOf[JdbcConfigDBModelV1]).get
    val makeProducer = (JdbcConfigModel.apply _).tupled
    makeProducer(values)
  }
}

object TelemetryConfigMapperSelector extends MapperSelector[TelemetryConfigModel, TelemetryConfigDBModel] {
  override def select(model: TelemetryConfigDBModel): Mapper[TelemetryConfigModel, TelemetryConfigDBModel] = {

    model match {
      case _: TelemetryConfigDBModelV1 => TelemetryConfigMapperV1
      case o                           => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")

    }
  }

  def applyMap(p: TelemetryConfigDBModel): TelemetryConfigModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object TelemetryConfigMapperV1 extends Mapper[TelemetryConfigModel, TelemetryConfigDBModelV1] {
  override val version = "telemetryConfigV1"

  override def fromModelToDBModel(p: TelemetryConfigModel): TelemetryConfigDBModelV1 = {

    val values      = TelemetryConfigModel.unapply(p).get
    val makeDBModel = (TelemetryConfigDBModelV1.apply _).tupled
    makeDBModel(values)
  }

  override def fromDBModelToModel[B >: TelemetryConfigDBModelV1](p: B): TelemetryConfigModel = {

    val values       = TelemetryConfigDBModelV1.unapply(p.asInstanceOf[TelemetryConfigDBModelV1]).get
    val makeProducer = (TelemetryConfigModel.apply _).tupled
    makeProducer(values)
  }
}

object NifiConfigMapperSelector extends MapperSelector[NifiConfigModel, NifiConfigDBModel] {
  override def select(model: NifiConfigDBModel): Mapper[NifiConfigModel, NifiConfigDBModel] = {

    model match {
      case _: NifiConfigDBModelV1 => NifiConfigMapperV1
      case o                      => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")

    }
  }

  def applyMap(p: NifiConfigDBModel): NifiConfigModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object NifiConfigMapperV1 extends Mapper[NifiConfigModel, NifiConfigDBModelV1] {
  override val version = "nifiConfigV1"

  override def fromModelToDBModel(p: NifiConfigModel): NifiConfigDBModelV1 = {

    val values      = NifiConfigModel.unapply(p).get
    val makeDBModel = (NifiConfigDBModelV1.apply _).tupled
    makeDBModel(values)
  }

  override def fromDBModelToModel[B >: NifiConfigDBModelV1](p: B): NifiConfigModel = {

    val values       = NifiConfigDBModelV1.unapply(p.asInstanceOf[NifiConfigDBModelV1]).get
    val makeProducer = (NifiConfigModel.apply _).tupled
    makeProducer(values)
  }
}

object CompilerConfigMapperSelector extends MapperSelector[CompilerConfigModel, CompilerConfigDBModel] {
  override def select(model: CompilerConfigDBModel): Mapper[CompilerConfigModel, CompilerConfigDBModel] = {

    model match {
      case _: CompilerConfigDBModelV1 => CompilerConfigMapperV1
      case o                          => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")

    }
  }

  def applyMap(p: CompilerConfigDBModel): CompilerConfigModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object CompilerConfigMapperV1 extends Mapper[CompilerConfigModel, CompilerConfigDBModelV1] {
  override val version = "nifiConfigV1"

  override def fromModelToDBModel(p: CompilerConfigModel): CompilerConfigDBModelV1 = {

    val values      = CompilerConfigModel.unapply(p).get
    val makeDBModel = (CompilerConfigDBModelV1.apply _).tupled
    makeDBModel(values)
  }

  override def fromDBModelToModel[B >: CompilerConfigDBModelV1](p: B): CompilerConfigModel = {

    val values       = CompilerConfigDBModelV1.unapply(p.asInstanceOf[CompilerConfigDBModelV1]).get
    val makeProducer = (CompilerConfigModel.apply _).tupled
    makeProducer(values)
  }
}
