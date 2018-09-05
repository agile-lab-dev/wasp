package it.agilelab.bigdata.wasp.consumers.spark.plugins.raw

import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkReader
import it.agilelab.bigdata.wasp.core.datastores.DatastoreProduct.RawProduct
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.RawModel
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}

class RawSparkReader(rawModel: RawModel) extends SparkReader with Logging {
  val name: String = rawModel.name
  val readerType: String = RawProduct.getActualProduct

  override def read(sc: SparkContext): DataFrame = {
    logger.info(s"Initialize Spark HDFSReader with this model: $rawModel")
    // get sql context
    val sqlContext = SQLContext.getOrCreate(sc)

    // setup reader
    val schema: StructType = DataType.fromJson(rawModel.schema).asInstanceOf[StructType]
    val options = rawModel.options
    val reader = sqlContext.read
      .schema(schema)
      .format(options.format)
      .options(options.extraOptions.getOrElse(Map()))

    // calculate path
    val path = if (rawModel.timed) {
      // the path is timed; find and return the most recent subdirectory
      val hdfsPath = new Path(rawModel.uri)
      val hdfs = hdfsPath.getFileSystem(sc.hadoopConfiguration)

      val subdirectories = hdfs.listStatus(hdfsPath)
        .toList
        .filter(_.isDirectory)
      val mostRecentSubdirectory = subdirectories.sortBy(_.getPath.getName)
        .reverse
        .head
        .getPath

      mostRecentSubdirectory.toString
    } else {
      // the path is not timed; return it as-is
      rawModel.uri
    }

    logger.info(s"Load this path: '$path'")

    // read
    reader.load(path)
  }
}

