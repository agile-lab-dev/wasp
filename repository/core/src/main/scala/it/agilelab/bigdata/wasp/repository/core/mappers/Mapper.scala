package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.repository.core.dbModels._

import scala.reflect.ClassTag

abstract class Mapper[T, +R](implicit tag: ClassTag[R]){
  val version: String


  // TODO: Generics implementation here?
  def fromModelToDBModel(m: T) : R
  def fromDBModelToModel[B >: R](m: B) : T

  def getDBModelType : Class[_] = tag.runtimeClass

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
    classOf[TelemetryConfigDBModelV1]       -> TelemetryConfigMapperV1.version)
}

trait MapperSelector[T, R]{
  def select(model: R): Mapper[T, _ <: R]
  def versionExtractor(m: R) : String = {
    val models = Models.models
    val version = models.getOrElse(m.getClass, throw new Exception("No VERSION"))
    version
  }
  def factory(p: R) : T = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}