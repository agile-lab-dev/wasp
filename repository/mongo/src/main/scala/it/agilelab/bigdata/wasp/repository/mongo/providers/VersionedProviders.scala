package it.agilelab.bigdata.wasp.repository.mongo.providers

import it.agilelab.bigdata.wasp.models.configuration.{CompilerConfigModel, ConnectionConfig, ElasticConfigModel, HBaseEntryConfig, JMXTelemetryConfigModel, JdbcConfigModel, JdbcConnectionConfig, JdbcPartitioningInfo, KafkaEntryConfig, KryoSerializerConfig, NifiConfigModel, NifiStatelessConfigModel, RestEnrichmentConfigModel, RestEnrichmentSource, RetainedConfigModel, SchedulingStrategyConfigModel, SparkBatchConfigModel, SparkDriverConfig, SparkEntryConfig, SparkStreamingConfigModel, TelemetryConfigModel, TelemetryTopicConfigModel, ZookeeperConnectionsConfig}
import it.agilelab.bigdata.wasp.models.{BatchETL, BatchETLModel, BatchGdprETLModel, BatchJobExclusionConfig, CdcOptions, ContainsRawMatchingStrategy, DashboardModel, ExactKeyValueMatchingStrategy, ExactRawMatchingStrategy, GenericOptions, KeyValueModel, KeyValueOption, LegacyStreamingETLModel, MlModelOnlyInfo, NoPartitionPruningStrategy, PrefixAndTimeBoundKeyValueMatchingStrategy, PrefixKeyValueMatchingStrategy, PrefixRawMatchingStrategy, RTModel, RawModel, RawOptions, ReaderModel, StrategyModel, StreamingReaderModel, StructuredStreamingETLModel, TimeBasedBetweenPartitionPruningStrategy, WriterModel}
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros.{createCodec, createCodecProvider, createCodecProviderIgnoreNone}
import it.agilelab.bigdata.wasp.repository.core.mappers._
import it.agilelab.bigdata.wasp.repository.core.dbModels._
import it.agilelab.bigdata.wasp.repository.mongo.TypesafeConfigCodecProvider
import it.agilelab.bigdata.wasp.repository.mongo.providers.DataStoreConfCodecProviders.{DataStoreConfCodecProvider, KeyValueDataStoreConfCodecProvider, KeyValueMatchingStrategyCodecProvider, PartitionPruningStrategyCodecProvider, RawDataStoreConfCodecProvider, RawMatchingStrategyCodecProvider}
import org.bson.codecs.Codec
import org.bson.codecs.configuration.CodecRegistry

object VersionedRegistry{
  val additionalCodecs: CodecRegistry = fromProviders(
    createCodecProviderIgnoreNone[RawModel](),
    createCodecProvider[CdcOptions](),
    createCodecProvider[RawOptions](),
    createCodecProvider[JdbcConnectionConfig](),
    createCodecProvider[ConnectionConfig](),
    createCodecProvider[NifiStatelessConfigModel](),
    createCodecProvider[SchedulingStrategyConfigModel](),
    createCodecProvider[SparkEntryConfig](),
    createCodecProvider[RetainedConfigModel](),
    createCodecProvider[KryoSerializerConfig](),
    createCodecProvider[SparkDriverConfig](),
    createCodecProvider[ZookeeperConnectionsConfig](),
    createCodecProvider[KafkaEntryConfig](),
    createCodecProvider[HBaseEntryConfig](),
    createCodecProvider[ReaderModel](),
    createCodecProvider[GenericOptions](),
    createCodecProvider[WriterModel](),
    createCodecProvider[BatchJobExclusionConfig](),
    createCodecProvider[StrategyModel](),
    createCodecProvider[KeyValueOption](),
    createCodecProviderIgnoreNone[LegacyStreamingETLModel](),
    createCodecProvider[StructuredStreamingETLModel](),
    createCodecProvider[StreamingReaderModel](),
    createCodecProvider[RestEnrichmentConfigModel](),
    createCodecProvider[RTModel](),
    createCodecProvider[TelemetryTopicConfigModel](),
    createCodecProvider[JMXTelemetryConfigModel](),
    createCodecProviderIgnoreNone(classOf[DashboardModel]),
    createCodecProviderIgnoreNone(classOf[RestEnrichmentSource]),
    createCodecProvider[JdbcPartitioningInfo](),
    BatchETLCodecProvider,
    DatastoreProductCodecProvider,
    TopicCompressionCodecProvider,
    HttpCompressionCodecProvider,
    SubjectStrategyCodecProvider,
    TypesafeConfigCodecProvider,
    PipegraphInstanceDBModelProvider,
    PipegraphInstanceDBModelProvider,
    BatchJobInstanceDBProvider,
    PartitionPruningStrategyCodecProvider,
    RawMatchingStrategyCodecProvider,
    KeyValueMatchingStrategyCodecProvider,
    DataStoreConfCodecProvider,
    RawDataStoreConfCodecProvider,
    KeyValueDataStoreConfCodecProvider
  )
  val codecRegistry: CodecRegistry = fromRegistries(
    additionalCodecs,
    DEFAULT_CODEC_REGISTRY
  )

  val producerDBModelV1Codec: Codec[ProducerDBModelV1] = createCodec[ProducerDBModelV1]()
  val producerDBModelV2Codec: Codec[ProducerDBModelV2] = createCodec[ProducerDBModelV2]()
  val ProducerDBProvider: VersionedCodecProvider[ProducerDBModel] =
    VersionedCodecProvider.apply(ProducerDBModelMapperSelector.versionExtractor, classOf[ProducerDBModel], (ProducerMapperV1.version, producerDBModelV1Codec), (ProducerMapperV2.version, producerDBModelV2Codec))

  val cdcDBModelCodecV1: Codec[CdcDBModelV1] = createCodec[CdcDBModelV1](codecRegistry)
  val CdcDBProvider: VersionedCodecProvider[CdcDBModel] =
    VersionedCodecProvider.apply(CdcMapperSelector.versionExtractor, classOf[CdcDBModel], (CdcMapperV1.version, cdcDBModelCodecV1))

  val rawDBModelV1Codec: Codec[RawDBModelV1] = createCodec[RawDBModelV1](codecRegistry)
  val RawDBProvider: VersionedCodecProvider[RawDBModel] =
    VersionedCodecProvider.apply(RawMapperSelector.versionExtractor, classOf[RawDBModel], (RawMapperV1.version, rawDBModelV1Codec))

  val sqlSourceDBModelV1Codec: Codec[SqlSourceDBModelV1] = createCodec[SqlSourceDBModelV1](codecRegistry)
  val SqlSourceProvider: VersionedCodecProvider[SqlSourceDBModel] =
    VersionedCodecProvider.apply(SqlSourceMapperSelector.versionExtractor, classOf[SqlSourceDBModel], (SqlSourceMapperV1.version, sqlSourceDBModelV1Codec))

  val keyValueDBModelV1Codec: Codec[KeyValueDBModelV1] = createCodec[KeyValueDBModelV1](codecRegistry)
  val KeyValueProvider: VersionedCodecProvider[KeyValueDBModel] =
    VersionedCodecProvider.apply(KeyValueMapperSelector.versionExtractor, classOf[KeyValueDBModel], (KeyValueMapperV1.version, keyValueDBModelV1Codec))

  val pipegraphDBModelV1Codec: Codec[PipegraphDBModelV1] = createCodec[PipegraphDBModelV1](codecRegistry)
  val PipegraphProvider: VersionedCodecProvider[PipegraphDBModel] =
    VersionedCodecProvider.apply(PipegraphDBModelMapperSelector.versionExtractor, classOf[PipegraphDBModel], (PipegraphMapperV1.version, pipegraphDBModelV1Codec))

  val indexDBModelV1Codec: Codec[IndexDBModelV1] = createCodec[IndexDBModelV1](codecRegistry)
  val IndexProvider: VersionedCodecProvider[IndexDBModel] =
    VersionedCodecProvider.apply(IndexDBModelMapperSelector.versionExtractor, classOf[IndexDBModel], (IndexMapperV1.version, indexDBModelV1Codec))

  val httpDBModelV1Codec: Codec[HttpDBModelV1] = createCodec[HttpDBModelV1](codecRegistry)
  val HttpProvider: VersionedCodecProvider[HttpDBModel] =
    VersionedCodecProvider.apply(HttpDBModelMapperSelector.versionExtractor, classOf[HttpDBModel], (HttpMapperV1.version, httpDBModelV1Codec))

  val documentDBModelV1Codec: Codec[DocumentDBModelV1] = createCodec[DocumentDBModelV1](codecRegistry)
  val DocumentProvider: VersionedCodecProvider[DocumentDBModel] =
    VersionedCodecProvider.apply(DocumentDBModelMapperSelector.versionExtractor, classOf[DocumentDBModel], (DocumentMapperV1.version, documentDBModelV1Codec))


  val batchJobProviders = fromProviders(
    BatchETLCodecProvider,
    createCodecProviderIgnoreNone[RawModel](),
    RawDBProvider,
    KeyValueProvider,
    createCodecProvider[RawOptions](),
    createCodecProvider[ExactRawMatchingStrategy](),
    createCodecProvider[PrefixRawMatchingStrategy](),
    createCodecProvider[ContainsRawMatchingStrategy](),
    createCodecProvider[TimeBasedBetweenPartitionPruningStrategy](),
    createCodecProvider[KeyValueModel](),
    createCodecProvider[ExactKeyValueMatchingStrategy](),
    createCodecProvider[PrefixKeyValueMatchingStrategy](),
    createCodecProvider[PrefixAndTimeBoundKeyValueMatchingStrategy](),
    createCodecProvider[NoPartitionPruningStrategy](),
    BatchGdprETLModelCodecProvider,
    BatchETLModelCodecProvider,
    createCodecProvider[BatchETLModel](),
    createCodecProvider[WriterModel](),
    createCodecProvider[ReaderModel](),
    createCodecProvider[BatchJobExclusionConfig](),
    createCodecProvider[StrategyModel],
    BatchGdprETLModelCodecProvider,
    PartitionPruningStrategyCodecProvider,
    RawMatchingStrategyCodecProvider,
    KeyValueMatchingStrategyCodecProvider,
    DataStoreConfCodecProvider,
    RawDataStoreConfCodecProvider,
    KeyValueDataStoreConfCodecProvider,
    createCodecProvider[MlModelOnlyInfo](),
    DatastoreProductCodecProvider
  )


  val batchJobRegistry = fromRegistries(batchJobProviders, DEFAULT_CODEC_REGISTRY)
  val batchJobDBModelV1Codec: Codec[BatchJobDBModelV1] = createCodec[BatchJobDBModelV1](batchJobRegistry)
  val BatchJobProvider: VersionedCodecProvider[BatchJobDBModel] =
    VersionedCodecProvider.apply(BatchJobModelMapperSelector.versionExtractor, classOf[BatchJobDBModel], (BatchJobMapperV1.version, batchJobDBModelV1Codec))

  val topicDBModelV1Codec: Codec[TopicDBModelV1] = createCodec[TopicDBModelV1](codecRegistry)
  val TopicProvider: VersionedCodecProvider[TopicDBModel] =
    VersionedCodecProvider.apply(TopicDBModelMapperSelector.versionExtractor, classOf[TopicDBModel], (TopicMapperV1.version, topicDBModelV1Codec))

  val genericDBModelV1Codec: Codec[GenericDBModelV1] = createCodec[GenericDBModelV1](codecRegistry)
  val GenericProvider: VersionedCodecProvider[GenericDBModel] =
    VersionedCodecProvider.apply(GenericMapperSelector.versionExtractor, classOf[GenericDBModel], (GenericMapperV1.version, genericDBModelV1Codec))

  val freeCodeDBModelV1Codec: Codec[FreeCodeDBModelV1] = createCodec[FreeCodeDBModelV1]()
  val FreeCodeProvider: VersionedCodecProvider[FreeCodeDBModel] =
    VersionedCodecProvider.apply(FreeCodeMapperSelector.versionExtractor, classOf[FreeCodeDBModel], (FreeCodeMapperV1.version, freeCodeDBModelV1Codec))

  val websocketDBModelV1Codec: Codec[WebsocketDBModelV1] = createCodec[WebsocketDBModelV1]()
  val WebsocketProvider: VersionedCodecProvider[WebsocketDBModel] =
    VersionedCodecProvider.apply(WebsocketMapperSelector.versionExtractor, classOf[WebsocketDBModel], (WebsocketMapperV1.version, websocketDBModelV1Codec))

  val batchSchedulerDBModelV1Codec: Codec[BatchSchedulerDBModelV1] = createCodec[BatchSchedulerDBModelV1]()
  val BatchSchedulerProvider: VersionedCodecProvider[BatchSchedulerDBModel] =
    VersionedCodecProvider.apply(BatchSchedulersMapperSelector.versionExtractor, classOf[BatchSchedulerDBModel], (BatchSchedulerMapperV1.version, batchSchedulerDBModelV1Codec))

  val processGroupDBModelV1Codec: Codec[ProcessGroupDBModelV1] = createCodec[ProcessGroupDBModelV1]()
  val ProcessGroupProvider: VersionedCodecProvider[ProcessGroupDBModel] =
    VersionedCodecProvider.apply(ProcessGroupMapperSelector.versionExtractor, classOf[ProcessGroupDBModel], (ProcessGroupMapperV1.version, processGroupDBModelV1Codec))

  val MlDBModelOnlyInfoV1Codec: Codec[MlDBModelOnlyInfoV1] = createCodec[MlDBModelOnlyInfoV1]()
  val MlModelOnlyInfoProvider: VersionedCodecProvider[MlDBModelOnlyInfo] =
    VersionedCodecProvider.apply(MlDBModelMapperSelector.versionExtractor, classOf[MlDBModelOnlyInfo], (MlDBModelMapperV1.version, MlDBModelOnlyInfoV1Codec))

  val MultiTopicDBModelV1Codec: Codec[MultiTopicDBModelV1] = createCodec[MultiTopicDBModelV1]()
  val MultiTopicProvider: VersionedCodecProvider[MultiTopicDBModel] =
    VersionedCodecProvider.apply(MultiTopicModelMapperSelector.versionExtractor, classOf[MultiTopicDBModel], (MultiTopicModelMapperV1.version, MultiTopicDBModelV1Codec))

  val solrConfigDBModelV1Codec: Codec[SolrConfigDBModelV1] = createCodec[SolrConfigDBModelV1](codecRegistry)
  val SolrConfigProvider: VersionedCodecProvider[SolrConfigDBModel] =
    VersionedCodecProvider.apply(SolrConfigMapperSelector.versionExtractor, classOf[SolrConfigDBModel], (SolrConfigMapperV1.version, solrConfigDBModelV1Codec))

  val hbaseConfigDBModelV1Codec: Codec[HBaseConfigDBModelV1] = createCodec[HBaseConfigDBModelV1](codecRegistry)
  val HBaseConfigProvider: VersionedCodecProvider[HBaseConfigDBModel] =
    VersionedCodecProvider.apply(HBaseConfigMapperSelector.versionExtractor, classOf[HBaseConfigDBModel], (HBaseConfigMapperV1.version, hbaseConfigDBModelV1Codec))

  val kafkaConfigDBModelV1Codec: Codec[KafkaConfigDBModelV1] = createCodec[KafkaConfigDBModelV1](codecRegistry)
  val KafkaConfigProvider: VersionedCodecProvider[KafkaConfigDBModel] =
    VersionedCodecProvider.apply(KafkaConfigMapperSelector.versionExtractor, classOf[KafkaConfigDBModel], (KafkaConfigMapperV1.version, kafkaConfigDBModelV1Codec))

  val sparkBatchConfigDBModelV1Codec: Codec[SparkBatchConfigDBModelV1] = createCodec[SparkBatchConfigDBModelV1](codecRegistry)
  val SparkBatchConfigProvider: VersionedCodecProvider[SparkBatchConfigDBModel] =
    VersionedCodecProvider.apply(
      SparkBatchConfigMapperSelector.versionExtractor,
      classOf[SparkBatchConfigDBModel],
      (SparkBatchConfigMapperV1.version, sparkBatchConfigDBModelV1Codec))

  val sparkStreamingConfigDBModelV1Codec: Codec[SparkStreamingConfigDBModelV1] = createCodec[SparkStreamingConfigDBModelV1](codecRegistry)
  val SparkStreamingConfigProvider: VersionedCodecProvider[SparkStreamingConfigDBModel] =
    VersionedCodecProvider.apply(
      SparkStreamingConfigMapperSelector.versionExtractor,
      classOf[SparkStreamingConfigDBModel],
      (SparkStreamingConfigMapperV1.version, sparkStreamingConfigDBModelV1Codec))

  val elasticConfigDBModelV1Codec: Codec[ElasticConfigDBModelV1] = createCodec[ElasticConfigDBModelV1](codecRegistry)
  val ElasticConfigProvider: VersionedCodecProvider[ElasticConfigDBModel] =
    VersionedCodecProvider.apply(ElasticConfigMapperSelector.versionExtractor, classOf[ElasticConfigDBModel], (ElasticConfigMapperV1.version, elasticConfigDBModelV1Codec))

  val jdbcConfigDBModelV1Codec: Codec[JdbcConfigDBModelV1] = createCodec[JdbcConfigDBModelV1](codecRegistry)
  val JdbcConfigProvider: VersionedCodecProvider[JdbcConfigDBModel] =
    VersionedCodecProvider.apply(JdbcConfigMapperSelector.versionExtractor, classOf[JdbcConfigDBModel], (JdbcConfigMapperV1.version, jdbcConfigDBModelV1Codec))

  val nifiConfigDBModelV1Codec: Codec[NifiConfigDBModelV1] = createCodec[NifiConfigDBModelV1]()
  val NifiConfigProvider: VersionedCodecProvider[NifiConfigDBModel] =
    VersionedCodecProvider.apply(NifiConfigMapperSelector.versionExtractor, classOf[NifiConfigDBModel], (NifiConfigMapperV1.version, nifiConfigDBModelV1Codec))

  val telemetryConfigDBModelV1Codec: Codec[TelemetryConfigDBModelV1] = createCodec[TelemetryConfigDBModelV1](VersionedRegistry.codecRegistry)
  val TelemetryConfigProvider: VersionedCodecProvider[TelemetryConfigDBModel] =
    VersionedCodecProvider.apply(TelemetryConfigMapperSelector.versionExtractor, classOf[TelemetryConfigDBModel], (TelemetryConfigMapperV1.version, telemetryConfigDBModelV1Codec))

  val compilerConfigDBModelV1Codec: Codec[CompilerConfigDBModelV1] = createCodec[CompilerConfigDBModelV1](VersionedRegistry.codecRegistry)
  val CompilerConfigProvider: VersionedCodecProvider[CompilerConfigDBModel] =
    VersionedCodecProvider.apply(CompilerConfigMapperSelector.versionExtractor, classOf[CompilerConfigDBModel], (CompilerConfigMapperV1.version, compilerConfigDBModelV1Codec))

}

