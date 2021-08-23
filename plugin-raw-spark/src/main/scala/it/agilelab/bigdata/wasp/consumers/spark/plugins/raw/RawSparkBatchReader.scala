package it.agilelab.bigdata.wasp.consumers.spark.plugins.raw

import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkBatchReader
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.hdfs.HdfsUtils
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.RawProduct
import it.agilelab.bigdata.wasp.models.RawModel
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}

class RawSparkBatchReader(rawModel: RawModel) extends SparkBatchReader with Logging {
  val name: String       = rawModel.name
  val readerType: String = RawProduct.getActualProductName

  override def read(sc: SparkContext): DataFrame = {
    logger.info(s"Initialize Spark HDFSReader with this model: $rawModel")
    // get sql context
    val sqlContext = SQLContext.getOrCreate(sc)

    // setup reader
    val schema: StructType = DataType.fromJson(rawModel.schema).asInstanceOf[StructType]
    val options            = rawModel.options
    val extraOptions       = options.extraOptions.getOrElse(Map())
    val reader = sqlContext.read
      .schema(schema)
      .format(options.format)
      .options(extraOptions)

    // calculate path
    val path = HdfsUtils.getRawModelPathToToLoad(rawModel, sc)

    logger.info(s"Load this path: '$path'")
    logger.info(RawSparkReaderUtils.printExtraOptions(rawModel.name, extraOptions))
    reader.load(path)
  }
}
