package it.agilelab.bigdata.wasp.consumers.spark.plugins.hbase

import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkBatchReader
import it.agilelab.bigdata.wasp.core.datastores.DatastoreProduct.HBaseProduct
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.KeyValueModel
import org.apache.spark.SparkContext
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SQLContext}

class HBaseSparkBatchReader(keyValueModel: KeyValueModel) extends SparkBatchReader with Logging {
  val name: String = keyValueModel.name
  val readerType: String = HBaseProduct.getActualProduct

  override def read(sc: SparkContext): DataFrame = {

    logger.info(s"Initialize Spark HBaseReader with this model: $keyValueModel")
    val sqlContext = SQLContext.getOrCreate(sc)
    val options: Map[String, String] = keyValueModel.getOptionsMap() ++
    Seq(
      HBaseTableCatalog.tableCatalog -> keyValueModel.tableCatalog,
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
