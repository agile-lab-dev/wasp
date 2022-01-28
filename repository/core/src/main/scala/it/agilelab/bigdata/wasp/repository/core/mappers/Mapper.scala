package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.repository.core.dbModels._

import scala.reflect.ClassTag
import shapeless.Generic

abstract class Mapper[T, +R](implicit tag: ClassTag[R]){
  val version: String

  def fromDBModelToModel[B >: R](m: B) : T
  def getDBModelType : Class[_] = tag.runtimeClass
}

trait SimpleMapper[T, R] extends Mapper[T,R]{
  override val version: String
  def transform[B] = new PartiallyApplied[B]

  class PartiallyApplied[B] {
    def apply[A, Repr](a: A)(implicit genA: Generic.Aux[A, Repr], genB: Generic.Aux[B, Repr]) = genB.from(genA.to(a))
  }
}

trait MapperSelector[T, R] {
  def select(model: R): Mapper[T, _ <: R] = {
    val version = versionExtractor(model)
    Mappers.mappers.getOrElse(version, throw new Exception(s"No version of mapper for model [$model]")).asInstanceOf[Mapper[T,_ <: R]]
  }


  def versionExtractor(m: R): String = {
    val models = Models.models
    val version = models.getOrElse(m.getClass, throw new Exception("No VERSION"))
    version
  }

  def factory(p: R): T = {
    val mapper: Mapper[T, _ <: R] = select(p)
    mapper.fromDBModelToModel(p)
  }
}

// FixMe: Change name
object Models{
  val models: Map[Object, String] = Map(
    classOf[ProducerDBModelV1]              -> ProducerMapperV1.version,
    classOf[ProducerDBModelV2]              -> ProducerMapperV2.version,
    classOf[IndexDBModelV1]                 -> IndexMapperV1.version,
    classOf[HttpDBModelV1]                  -> HttpMapperV1.version,
    classOf[PipegraphInstanceDBModelV1]     -> PipegraphInstanceMapperV1.version,
    classOf[PipegraphDBModelV1]             -> PipegraphMapperV1.version,
    classOf[DocumentDBModelV1]              -> DocumentMapperV1.version,
    classOf[KeyValueDBModelV1]              -> KeyValueMapperV1.version,
    classOf[TopicDBModelV1]                 -> TopicMapperV1.version,
    classOf[CdcDBModelV1]                   -> CdcMapperV1.version,
    classOf[RawDBModelV1]                   -> RawMapperV1.version,
    classOf[SqlSourceDBModelV1]             -> SqlSourceMapperV1.version,
    classOf[BatchJobInstanceDBModelV1]      -> BatchJobInstanceMapperV1.version,
    classOf[BatchJobDBModelV1]              -> BatchJobMapperV1.version,
    classOf[GenericDBModelV1]               -> GenericMapperV1.version,
    classOf[FreeCodeDBModelV1]              -> FreeCodeMapperV1.version,
    classOf[WebsocketDBModelV1]             -> WebsocketMapperV1.version,
    classOf[BatchSchedulerDBModelV1]        -> BatchSchedulerMapperV1.version,
    classOf[ProcessGroupDBModelV1]          -> ProcessGroupMapperV1.version,
    classOf[MlDBModelOnlyInfoV1]            -> MlDBModelMapperV1.version,
    classOf[MultiTopicDBModelV1]            -> MultiTopicModelMapperV1.version,
    classOf[SolrConfigDBModelV1]            -> SolrConfigMapperV1.version,
    classOf[HBaseConfigDBModelV1]           -> HBaseConfigMapperV1.version,
    classOf[KafkaConfigDBModelV1]           -> KafkaConfigMapperV1.version,
    classOf[SparkBatchConfigDBModelV1]      -> SparkBatchConfigMapperV1.version,
    classOf[SparkStreamingConfigDBModelV1]  -> SparkStreamingConfigMapperV1.version,
    classOf[NifiConfigDBModelV1]            -> NifiConfigMapperV1.version,
    classOf[CompilerConfigDBModelV1]        -> CompilerConfigMapperV1.version,
    classOf[JdbcConfigDBModelV1]            -> JdbcConfigMapperV1.version,
    classOf[TelemetryConfigDBModelV1]       -> TelemetryConfigMapperV1.version,
    classOf[ElasticConfigDBModelV1]         -> ElasticConfigMapperV1.version)
}

object Mappers{
  val mappers: Map[String, Object] = Map(
    ProducerMapperV1.version              -> ProducerMapperV1,
    ProducerMapperV2.version              -> ProducerMapperV2,
    IndexMapperV1.version                 -> IndexMapperV1,
    HttpMapperV1.version                  -> HttpMapperV1,
    PipegraphInstanceMapperV1.version     -> PipegraphInstanceMapperV1,
    PipegraphMapperV1.version             -> PipegraphMapperV1,
    DocumentMapperV1.version              -> DocumentMapperV1,
    KeyValueMapperV1.version              -> KeyValueMapperV1,
    TopicMapperV1.version                 -> TopicMapperV1 ,
    CdcMapperV1.version                   -> CdcMapperV1,
    RawMapperV1.version                   -> RawMapperV1 ,
    SqlSourceMapperV1.version             -> SqlSourceMapperV1,
    BatchJobInstanceMapperV1.version      -> BatchJobInstanceMapperV1,
    BatchJobMapperV1.version              -> BatchJobMapperV1 ,
    GenericMapperV1.version               -> GenericMapperV1,
    FreeCodeMapperV1.version              -> FreeCodeMapperV1,
    WebsocketMapperV1.version             -> WebsocketMapperV1,
    BatchSchedulerMapperV1.version        -> BatchSchedulerMapperV1,
    ProcessGroupMapperV1.version          -> ProcessGroupMapperV1,
    MlDBModelMapperV1.version             -> MlDBModelMapperV1,
    MultiTopicModelMapperV1.version       -> MultiTopicModelMapperV1,
    SolrConfigMapperV1.version            -> SolrConfigMapperV1,
    HBaseConfigMapperV1.version           -> HBaseConfigMapperV1,
    KafkaConfigMapperV1.version           -> KafkaConfigMapperV1,
    SparkBatchConfigMapperV1.version      -> SparkBatchConfigMapperV1,
    SparkStreamingConfigMapperV1.version  -> SparkStreamingConfigMapperV1,
    NifiConfigMapperV1.version            -> NifiConfigMapperV1,
    CompilerConfigMapperV1.version        -> CompilerConfigMapperV1,
    JdbcConfigMapperV1.version            -> JdbcConfigMapperV1,
    TelemetryConfigMapperV1.version       -> TelemetryConfigMapperV1,
    ElasticConfigMapperV1.version         -> ElasticConfigMapperV1)
}