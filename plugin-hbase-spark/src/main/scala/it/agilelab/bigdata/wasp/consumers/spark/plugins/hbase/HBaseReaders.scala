package it.agilelab.bigdata.wasp.consumers.spark.plugins.hbase

import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkReader
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.{KeyValueModel, TopicModel}
import org.apache.spark.SparkContext
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SQLContext}


object HBaseReaders {
  def createHBaseReader(model: KeyValueModel, name: String): SparkReader = {
    new HBaseReader(model)
  }
}

class HBaseReader(model: KeyValueModel) extends SparkReader with Logging {
  override val name = model.name
  override val readerType = "hbase"

  override def read(sc: SparkContext): DataFrame = {
    logger.info(s"Initialize Spark HBaseReader with this model: $model")
    val sqlContext = SQLContext.getOrCreate(sc)
    val options: Map[String, String] = model.getOptionsMap() ++
    Seq(
      HBaseTableCatalog.tableCatalog -> model.tableCatalog,
      //TODO fix me
      KeyValueModel.metadataAvroSchemaKey -> "",
      HBaseTableCatalog.newTable -> "4"
    )

    sqlContext
      .read
      .options(options)
      .format("org.apache.hadoop.hbase.spark")
      .load()

  }

}
