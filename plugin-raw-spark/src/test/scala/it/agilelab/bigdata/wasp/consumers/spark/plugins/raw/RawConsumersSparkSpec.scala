package it.agilelab.bigdata.wasp.consumers.spark.plugins.raw

import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import it.agilelab.bigdata.wasp.models.{RawModel, RawOptions, StreamingReaderModel, StructuredStreamingETLModel}
import it.agilelab.bigdata.wasp.repository.core.bl.RawBL
import org.scalatest.FunSuite

import scala.collection.immutable.Map

class RawConsumersSparkSpec extends FunSuite with SparkSuite {

  test("create a rate stream") {
    val rateModel: RawModel = RawModel(
      name = "rate",
      uri = "",
      timed = false,
      schema = "",
      options = RawOptions(
        saveMode = "append",
        format = "rate",
        extraOptions = Some(Map[String, String]("rowsPerSecond" -> "10"))
      )
    )
    val reader = StreamingReaderModel.rawReader("raw reader", rateModel)
    val etl: StructuredStreamingETLModel = StructuredStreamingETLModel(
      name = "",
      group = "",
      streamingInput = reader,
      staticInputs = Nil,
      streamingOutput = null,
      mlModels = Nil,
      strategy = None,
      triggerIntervalMs = None
    )
    val target = new RawConsumersSpark()
    target.rawBL = new RawBL {
      override def getByName(name: String): Option[RawModel] = Some(rateModel)

      override def persist(rawModel: RawModel): Unit = ()

      override def upsert(rawModel: RawModel): Unit = ()

      override def getAll(): Seq[RawModel] = Seq(rateModel)
    }
    val df = target
      .getSparkStructuredStreamingReader(
        spark,
        etl,
        reader
      )
      .createStructuredStream(etl, reader)(spark)
    val query = df.writeStream.format("console").start()
    query.stop()
  }
}
