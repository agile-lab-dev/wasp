package it.agilelab.bigdata.wasp.consumers.spark.plugins.raw

import it.agilelab.bigdata.wasp.consumers.spark.readers.StaticReader
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.RawModel
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}

class HDFSReader(hdfsModel: RawModel) extends StaticReader with Logging {
  override val name: String = hdfsModel.name
  override val readerType: String = "hdfs"

  override def read(sc: SparkContext): DataFrame = {
    logger.info(s"Initialize Spark HDFSReader with this model: $hdfsModel")
    // get sql context
    val sqlContext = SQLContext.getOrCreate(sc)

    // setup reader
    val schema: StructType = DataType.fromJson(hdfsModel.schema).asInstanceOf[StructType]
    val options = hdfsModel.options
    val reader = sqlContext.read
      .schema(schema)
      .format(options.format)
      .options(options.extraOptions.getOrElse(Map()))

    // calculate path
    val path = if (hdfsModel.timed) {
      // the path is timed; find and return the most recent subdirectory
      val hdfsPath = new Path(hdfsModel.uri)
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
      hdfsModel.uri
    }

    logger.info(s"Load this path: '$path'")

    // read
    reader.load(path)
  }
}

