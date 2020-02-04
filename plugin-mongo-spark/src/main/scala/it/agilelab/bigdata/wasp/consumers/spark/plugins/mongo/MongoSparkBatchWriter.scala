package it.agilelab.bigdata.wasp.consumers.spark.plugins.mongo

import com.mongodb.spark.config.WriteConfig
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkBatchWriter
import it.agilelab.bigdata.wasp.core.models.{DocumentModel, WriterModel}
import org.apache.spark.sql.DataFrame
import com.mongodb.spark.sql._

class MongoSparkBatchWriter(writer: WriterModel, model : DocumentModel) extends SparkBatchWriter {
  override def write(data: DataFrame): Unit = {

    val conf = data.sparkSession.sparkContext.getConf.clone()

    writer.options.foreach{
      case (key, value) => conf.set(key, value)
    }

    conf.set("spark.mongodb.output.uri", model.connectionString)


    val mongoWriteConf = WriteConfig(conf)

    data.write.mongo(mongoWriteConf)
  }
}
