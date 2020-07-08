package it.agilelab.bigdata.wasp.db.mongo.providers

import java.util

import it.agilelab.bigdata.wasp.core.models._
import org.apache.spark.sql.types._
import org.bson.BsonDocumentWriter
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}
import org.bson.codecs.{DecoderContext, EncoderContext}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros.createCodecProviderIgnoreNone
import org.mongodb.scala.bson.{BsonDocument, BsonObjectId}
import org.scalatest.FunSuite

import scala.collection.JavaConverters._

class BatchETLCodecProviderTest extends FunSuite {

  val gdprCodecProviders: util.List[CodecProvider] = List(
    DatastoreProductCodecProvider,
    createCodecProviderIgnoreNone(classOf[ExactKeyValueMatchingStrategy]),
    createCodecProviderIgnoreNone(classOf[PrefixKeyValueMatchingStrategy]),
    createCodecProviderIgnoreNone(classOf[PrefixAndTimeBoundKeyValueMatchingStrategy]),
    createCodecProviderIgnoreNone(classOf[TimeBasedBetweenPartitionPruningStrategy]),
    createCodecProviderIgnoreNone(classOf[NoPartitionPruningStrategy]),
    createCodecProviderIgnoreNone(classOf[ExactRawMatchingStrategy]),
    createCodecProviderIgnoreNone(classOf[PrefixRawMatchingStrategy]),
    DataStoreConfCodecProviders.PartitionPruningStrategyCodecProvider,
    DataStoreConfCodecProviders.RawMatchingStrategyCodecProvider,
    DataStoreConfCodecProviders.KeyValueMatchingStrategyCodecProvider,
    DataStoreConfCodecProviders.DataStoreConfCodecProvider,
    DataStoreConfCodecProviders.RawDataStoreConfCodecProvider,
    DataStoreConfCodecProviders.KeyValueDataStoreConfCodecProvider,
    createCodecProviderIgnoreNone(classOf[KeyValueDataStoreConf]),
    BatchGdprETLModelCodecProvider,
    createCodecProviderIgnoreNone(classOf[BatchETLModel]),
    createCodecProviderIgnoreNone(classOf[RawOptions]),
    createCodecProviderIgnoreNone(classOf[RawModel]),
    createCodecProviderIgnoreNone(classOf[ReaderModel]),
    createCodecProviderIgnoreNone(classOf[MlModelOnlyInfo]),
    createCodecProviderIgnoreNone(classOf[StrategyModel]),
    createCodecProviderIgnoreNone(classOf[WriterModel]),
    createCodecProviderIgnoreNone(classOf[BatchJobExclusionConfig]),
    BatchETLCodecProvider,
    BatchJobModelCodecProvider
  ).asJava

  val registry: CodecRegistry = fromRegistries(
    fromProviders(gdprCodecProviders),
    DEFAULT_CODEC_REGISTRY
  )

  test("BatchJobModel codec provider should be able to handle encoding and decoding") {
    val hostname = "host"
    lazy val dataRawModel: RawModel = RawModel(
      name = "GdprDataRawModel",
      uri = s"hdfs://$hostname:9000/user/root/gdpr/data",
      schema = StructType(Seq(
        StructField("id", StringType),
        StructField("number", LongType),
        StructField("name", StringType)
      )).json)

    val dataStores: List[DataStoreConf] = List(
      RawDataStoreConf("id", "correlationId", dataRawModel, ExactRawMatchingStrategy("key"), NoPartitionPruningStrategy())
    )

    lazy val inputRawModel: RawModel = RawModel(
      name = "GdprInputRawModel",
      uri = s"hdfs://$hostname:9000/user/root/gdpr/input",
      schema = StructType(Seq(StructField("key", StringType))).json)

    lazy val inputs = List(
      ReaderModel.rawReader(
        name = "GdprInputRawModel",
        rawModel = inputRawModel
      )
    )

    lazy val outputRawModel: RawModel = RawModel(
      name = "GdprOutputRawModel",
      uri = s"hdfs://$hostname:9000/user/root/gdpr/result/",
      schema = StructType(Seq(
        StructField("key", StringType),
        StructField("result", BooleanType)
      )).json)

    lazy val output: WriterModel = WriterModel.rawWriter(
      name = "GdprOutputRawModel",
      rawModel = outputRawModel
    )

    lazy val batchETL = BatchGdprETLModel(
      name = "gpdrETLModel",
      dataStores = dataStores,
      strategyConfig = "",
      inputs = inputs,
      output = output
    )

    lazy val model: BatchJobModel = BatchJobModel(
      name = "gdprBatchJob",
      description = "gdpr deletion test model",
      owner = "user",
      system = false,
      creationTime = System.currentTimeMillis(),
      etl = batchETL
    )

    val writer = new BsonDocumentWriter(new BsonDocument("_id", new BsonObjectId))
    registry.get(classOf[BatchJobModel]).encode(writer, model, EncoderContext.builder().isEncodingCollectibleDocument(true).build())

    val doc = writer.getDocument

    val reader = writer.getDocument.asBsonReader()
    val modelDecoded = registry.get(classOf[BatchJobModel]).decode(reader, DecoderContext.builder.build())

    assert(model === modelDecoded)

  }

}
