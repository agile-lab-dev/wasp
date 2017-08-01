package it.agilelab.bigdata.wasp.core.models

import it.agilelab.bigdata.wasp.core.models.configuration._
import it.agilelab.bigdata.wasp.core.utils.ConnectionConfig
import reactivemongo.bson._

trait BSONConversionHelper {

  implicit val readerBatchJob: BSONDocumentReader[BatchJobModel] = Macros.reader[BatchJobModel]
  implicit val writerBatchJob: BSONDocumentWriter[BatchJobModel] = Macros.writer[BatchJobModel]

  implicit val readerTopic: BSONDocumentReader[TopicModel] = Macros.reader[TopicModel]
  implicit val writerTopic: BSONDocumentWriter[TopicModel] = Macros.writer[TopicModel]

  implicit val readerIndex: BSONDocumentReader[IndexModel] = Macros.reader[IndexModel]
  implicit val writerIndex: BSONDocumentWriter[IndexModel] = Macros.writer[IndexModel]

  implicit val readerStrategy: BSONDocumentReader[StrategyModel] = Macros.reader[StrategyModel]
  implicit val writerStrategy: BSONDocumentWriter[StrategyModel] = Macros.writer[StrategyModel]

  implicit val readerDashboard: BSONDocumentReader[DashboardModel] = Macros.reader[DashboardModel]
  implicit val writerDashboard: BSONDocumentWriter[DashboardModel] = Macros.writer[DashboardModel]

  implicit val readerReaderModel: BSONDocumentReader[ReaderModel] = Macros.reader[ReaderModel]
  implicit val writerReaderModel: BSONDocumentWriter[ReaderModel] = Macros.writer[ReaderModel]

  implicit val readerWriterModel: BSONDocumentReader[WriterModel] = Macros.reader[WriterModel]
  implicit val writerWriterModel: BSONDocumentWriter[WriterModel] = Macros.writer[WriterModel]

  implicit val readerETL: BSONDocumentReader[ETLModel] = Macros.reader[ETLModel]
  implicit val writerETL: BSONDocumentWriter[ETLModel] = Macros.writer[ETLModel]

  implicit val readerRT: BSONDocumentReader[RTModel] = Macros.reader[RTModel]
  implicit val writerRT: BSONDocumentWriter[RTModel] = Macros.writer[RTModel]

  implicit val readerProducer: BSONDocumentReader[ProducerModel] = Macros.reader[ProducerModel]
  implicit val writerProducer: BSONDocumentWriter[ProducerModel] = Macros.writer[ProducerModel]

  implicit val readerPipeline: BSONDocumentReader[PipegraphModel] = Macros.reader[PipegraphModel]
  implicit val writerPipeline: BSONDocumentWriter[PipegraphModel] = Macros.writer[PipegraphModel]

  implicit val readerZooConfig: BSONDocumentReader[ConnectionConfig] = Macros.reader[ConnectionConfig]
  implicit val writerZooConfig: BSONDocumentWriter[ConnectionConfig] = Macros.writer[ConnectionConfig]

  implicit val readerKafkaConfig: BSONDocumentReader[KafkaConfigModel] = Macros.reader[KafkaConfigModel]
  implicit val writerKafkaConfig: BSONDocumentWriter[KafkaConfigModel] = Macros.writer[KafkaConfigModel]

  implicit val readerSparkBatchConfig: BSONDocumentReader[SparkBatchConfigModel] = Macros.reader[SparkBatchConfigModel]
  implicit val writerSparkBatchConfig: BSONDocumentWriter[SparkBatchConfigModel] = Macros.writer[SparkBatchConfigModel]

  implicit val readerSparkStreamingConfig: BSONDocumentReader[SparkStreamingConfigModel] = Macros.reader[SparkStreamingConfigModel]
  implicit val writerSparkStreamingConfig: BSONDocumentWriter[SparkStreamingConfigModel] = Macros.writer[SparkStreamingConfigModel]

  implicit val readerElasticConfig: BSONDocumentReader[ElasticConfigModel] = Macros.reader[ElasticConfigModel]
  implicit val writerElasticConfig: BSONDocumentWriter[ElasticConfigModel] = Macros.writer[ElasticConfigModel]

  implicit val readerSolrConfig: BSONDocumentReader[SolrConfigModel] = Macros.reader[SolrConfigModel]
  implicit val writerSolrConfig: BSONDocumentWriter[SolrConfigModel] = Macros.writer[SolrConfigModel]

  implicit val readerMlModel: BSONDocumentReader[MlModelOnlyInfo] = Macros.reader[MlModelOnlyInfo]
  implicit val writerMlModel: BSONDocumentWriter[MlModelOnlyInfo] = Macros.writer[MlModelOnlyInfo]

  implicit val readerWebsocketModel: BSONDocumentReader[WebsocketModel] = Macros.reader[WebsocketModel]
  implicit val writerWebsocketModel: BSONDocumentWriter[WebsocketModel] = Macros.writer[WebsocketModel]
  
  implicit val readerRawModel: BSONDocumentReader[RawModel] = Macros.reader[RawModel]
  implicit val writerRawModel: BSONDocumentWriter[RawModel] = Macros.writer[RawModel]
  
  implicit val readerRawOptions: BSONDocumentReader[RawOptions] = Macros.reader[RawOptions]
  implicit val writerRawOptions: BSONDocumentWriter[RawOptions] = Macros.writer[RawOptions]

  implicit val readerKeyValueModel: BSONDocumentReader[KeyValueModel] = Macros.reader[KeyValueModel]
  implicit val writerKeyValueModel: BSONDocumentWriter[KeyValueModel] = Macros.writer[KeyValueModel]
  
  implicit val readerBatchSchedulerModel: BSONDocumentReader[BatchSchedulerModel] = Macros.reader[BatchSchedulerModel]
  implicit val writerBatchSchedulerModel: BSONDocumentWriter[BatchSchedulerModel] = Macros.writer[BatchSchedulerModel]

  implicit val readerWriteType: BSONDocumentReader[WriteType] = Macros.reader[WriteType]
  implicit val writerWriteType: BSONDocumentWriter[WriteType] = Macros.writer[WriteType]
  
  /** A reader for Map[String, String]
   */
  implicit def readerMap: BSONDocumentReader[Map[String, String]] = new BSONDocumentReader[Map[String, String]] {
    def read(bson: BSONDocument): Map[String, String] = {
      val elements = bson.elements.map {
        case (field, value) => field -> value.seeAsTry[String].get
      }
      elements.toMap
    }
  }
  
  /** A writer for Map[String, String]
    */
  implicit def writerMap: BSONDocumentWriter[Map[String, String]] = new BSONDocumentWriter[Map[String, String]] {
    def write(map: Map[String, String]): BSONDocument = {
      val elements = map.map {
        case (key, value) => key -> BSONString(value)
      }
      BSONDocument(elements)
    }
  }
}