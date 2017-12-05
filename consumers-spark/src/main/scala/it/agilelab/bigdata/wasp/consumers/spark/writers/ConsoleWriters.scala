package it.agilelab.bigdata.wasp.consumers.spark.writers

import it.agilelab.bigdata.wasp.consumers.spark.SparkSingletons
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.streaming.dstream.DStream

class ConsoleLegacyStreamingWriter()
  extends SparkLegacyStreamingWriter {

  override def write(stream: DStream[String]): Unit = {

      stream.foreachRDD(rdd => {

        val sqlContext = SparkSingletons.getSQLContext

        import sqlContext.implicits._
        val dfWithSchema = sqlContext.read.json(rdd.toDS())

        ConsoleWriters.write(dfWithSchema)
      })
  }
}

class ConsoleSparkStructuredStreamingWriter()
  extends SparkStructuredStreamingWriter {

  override def write(stream: DataFrame, queryName: String, checkpointDir: String): Unit = ConsoleWriters.write(stream)
}

class ConsoleSparkWriter extends SparkWriter {

  override def write(data: DataFrame): Unit = ConsoleWriters.write(data)
}

object ConsoleWriters {

  def write(dataframe: DataFrame): Unit = dataframe.show()

}