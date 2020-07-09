package it.agilelab.bigdata.wasp.consumers.spark.plugins.mongo

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import it.agilelab.bigdata.wasp.consumers.spark.SparkSingletons
import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkBatchReader
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.MongoDbProduct
import it.agilelab.bigdata.wasp.models.{DocumentModel, ReaderModel}
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StructType}

class MongoSparkBatchReader(reader: ReaderModel, model: DocumentModel) extends SparkBatchReader {

  override val name: String = model.name
  override val readerType: String = MongoDbProduct.getActualProductName

  override def read(sc: SparkContext): DataFrame = {

    val conf = sc.getConf.clone()

    reader.options.foreach{
      case (key, value) => conf.set(key, value)
    }

    conf.set("spark.mongodb.input.uri", model.connectionString)


    val schema = DataType.fromJson(model.schema).asInstanceOf[StructType]

    val mongoReadConf = ReadConfig(conf)

    MongoSpark.builder().sparkSession(SparkSingletons.getSparkSession).readConfig(mongoReadConf).build().toDF(schema)

  }
}
