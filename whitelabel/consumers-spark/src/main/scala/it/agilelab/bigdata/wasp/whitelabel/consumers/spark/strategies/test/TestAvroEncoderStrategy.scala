package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test

import com.sksamuel.avro4s.{AvroSchema, FromRecord, ToRecord}
import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.consumers.spark.utils.AvroEncoders
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestAvroEncoderStrategy._
import it.agilelab.bigdata.wasp.whitelabel.models.test.{TestDocumentEncoder, TestState}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders}

class TestAvroEncoderStrategy extends Strategy {

  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {
    val df = dataFrames.head._2
    import df.sparkSession.implicits._
    df.as[TestDocumentEncoder].groupByKey(_.id)
      .flatMapGroupsWithState[TestState, String](OutputMode.Append(), GroupStateTimeout.NoTimeout()) {
        case (key, values, state) =>
          lazy val list = values.toList
          state.update(TestState(list.size, list.map(_.nested), list.size.toString))
          Iterator(key)
      }(encoder, Encoders.STRING).toDF()
  }

}

object TestAvroEncoderStrategy {

  val schema1: Schema = AvroSchema[TestState]
  val toRecord1: TestState => GenericRecord = ToRecord[TestState].apply(_)
  val fromRecord1: GenericRecord => TestState = FromRecord[TestState].apply(_)

  val encoder: Encoder[TestState] = AvroEncoders.avroEncoder(
    schema1,
    ConfigManager.getAvroSchemaManagerConfig,
    toRecord1,
    fromRecord1
  )

}