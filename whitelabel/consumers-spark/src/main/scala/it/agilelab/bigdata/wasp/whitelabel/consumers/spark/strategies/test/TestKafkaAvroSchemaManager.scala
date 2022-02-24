package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test

import it.agilelab.bigdata.wasp.consumers.spark.SparkSingletons
import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.GenericTopicProduct
import it.agilelab.bigdata.wasp.whitelabel.models.test.{AvroSchemaManagerHbase, TestSchemaAvroManager, TopicAvro_v2}
import org.apache.spark.sql.DataFrame

/**
  * @author andreaL
  */

class TestKafkaReaderWithDifferentVersionOfAvro extends Strategy {

  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {

    dataFrames(ReaderKey(GenericTopicProduct.categoryName, TestSchemaAvroManager.topicAvro_v3.name)).drop("kafkaMetadata")

  }
}

class TestHBaseWithSchemaAvroManagerv2 extends Strategy {

  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {

    val input = dataFrames(ReaderKey(GenericTopicProduct.categoryName, TestSchemaAvroManager.topicAvro_v2.name)).drop("kafkaMetadata")
    val ss = SparkSingletons.getSparkSession

    import ss.implicits._

    val version = configuration.getString("v")

    input
      .as[TopicAvro_v2]
      .map( x=> AvroSchemaManagerHbase(s"${version}-${x.id.substring(0,7)}", x))
      .toDF()

  }

}