package it.agilelab.bigdata.wasp.consumers.spark.plugins.console

import it.agilelab.bigdata.wasp.consumers.spark.SparkSingletons
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkLegacyStreamingWriter, SparkStructuredStreamingWriter, SparkBatchWriter}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.streaming.dstream.DStream

class ConsoleSparkLegacyStreamingWriter()
  extends SparkLegacyStreamingWriter {

  override def write(stream: DStream[String]): Unit = {

      stream.foreachRDD(rdd => {
        //if (!rdd.isEmpty()) {

          val sqlContext = SparkSingletons.getSQLContext

          //val df= sqlContext.read.json(rdd) // deprecated
          import sqlContext.implicits._
          val df = sqlContext.read.json(rdd.toDS())

          ConsoleWriters.write(df)
        }
      )
  }
}

class ConsoleSparkStructuredStreamingWriter()
  extends SparkStructuredStreamingWriter {

  override def write(stream: DataFrame): DataStreamWriter[Row] = {
    // configure and start streaming
    stream.writeStream
      .format("console")
      .outputMode("append")
  }
}

class ConsoleSparkBatchWriter extends SparkBatchWriter {

  override def write(data: DataFrame): Unit = ConsoleWriters.write(data)

}

object ConsoleWriters {

  def write(dataframe: DataFrame): Unit = dataframe.show() // truncate

}