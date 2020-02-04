package it.agilelab.bigdata.wasp.consumers.spark.plugins.mongo

import com.mongodb.spark.sql.MongoForeachRddWriter
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkStructuredStreamingWriter
import it.agilelab.bigdata.wasp.core.models.{DocumentModel, WriterModel}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, Row}

class MongoSparkStructuredStreamingWriter(writer: WriterModel, model: DocumentModel) extends SparkStructuredStreamingWriter {
  override def write(stream: DataFrame): DataStreamWriter[Row] = {



      val conf = stream.sparkSession.sparkContext.getConf.clone()

      writer.options.foreach{
        case (key, value) => conf.set(key, value)
      }

      conf.set("spark.mongodb.output.uri", model.connectionString)

      stream.writeStream
            .foreach(MongoForeachRddWriter(conf, stream.schema))

  }
}
