package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import org.apache.spark.sql.DataFrame

case class NestedObject(field1: String, field2: Long, field3: String)
case class TestObject(id: String, number: Option[Int], nested: Option[NestedObject], error: Option[String], topic: String)

class TestMultiTopicWriteMixedStrategy extends Strategy {

  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {


    import org.apache.spark.sql.functions._

    val dataFrame = dataFrames.head._2
    import dataFrame.sparkSession.implicits._

    val dataset = dataFrames.head._2.select("id", "number", "nested")
      .withColumn("error", typedLit[Option[String]](None))
      .withColumn("topic", typedLit[Option[String]](None))
      .as[TestObject]

    dataset.map{
      obj => if (obj.nested.get.field2 % 2 != 0) {
        obj.copy(
          error = Some("Error" + obj.nested.get.field2),
          nested = None,
          topic = "test_multi1.topic"
        )
      } else {
        obj.copy(
          number = None,
          topic = "test_multi2.topic"
        )
      }
    }.toDF()

  }

  }
