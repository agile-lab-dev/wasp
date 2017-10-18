package it.agilelab.bigdata.wasp.consumers.spark.plugins.raw

import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkStreamingWriter, SparkStructuredStreamingWriter, SparkWriter}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.RawModel
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

class HDFSSparkStreamingWriter(hdfsModel: RawModel,
                               ssc: StreamingContext)
  extends SparkStreamingWriter with Logging {

  override def write(stream: DStream[String]): Unit = {
    // get sql context
    val sqlContext = SQLContext.getOrCreate(ssc.sparkContext)
    // To avoid task not serializeble of spark
    val hdfsModelLocal = hdfsModel
    logger.info(s"Initialize DStream HDFS writer: $hdfsModel")
    stream.foreachRDD {
      rdd =>
        if (!rdd.isEmpty()) {


          // create df from rdd using provided schema & spark's json datasource
          val schema: StructType = DataType.fromJson(hdfsModelLocal.schema).asInstanceOf[StructType]
          val df = sqlContext.read.schema(schema).json(rdd)

          // calculate path
          val path = if (hdfsModelLocal.timed) {
            // the path must be timed; add timed subdirectory
            val hdfsPath = new Path(hdfsModelLocal.uri)
            val timedPath = new Path(hdfsPath.toString + "/" + ConfigManager.buildTimedName("").substring(1) + "/")

            timedPath.toString
          } else {
            // the path is not timed; return it as-is
            hdfsModelLocal.uri
          }

          // get options
          val options = hdfsModelLocal.options
          val mode = if (options.saveMode == "default") "append" else options.saveMode
          val format = options.format
          val extraOptions = options.extraOptions.getOrElse(Map())

          // setup writer
          val writer = df.write
            .mode(mode)
            .format(format)
            .options(extraOptions)

          // write
          writer.save(path)
        }
    }
  }
}

class HDFSSparkStructuredStreamingWriter(hdfsModel: RawModel,
                                         ss: SparkSession)
  extends SparkStructuredStreamingWriter with Logging {

  override def write(stream: DataFrame, queryName: String, checkpointDir: String): Unit = {

    // get options
    val extraOptions = hdfsModel.options.extraOptions.getOrElse(Map())
    // get save mode
    val mode = if (hdfsModel.options.saveMode == "default") "append" else hdfsModel.options.saveMode
    // get format
    val format = hdfsModel.options.format
    // get path timed or standard
    val path = if (hdfsModel.timed) {
      // the path must be timed; add timed subdirectory
      val hdfsPath = new Path(hdfsModel.uri)
      val timedPath = new Path(hdfsPath.toString + "/" + ConfigManager.buildTimedName("").substring(1) + "/")

      timedPath.toString
    } else {
      // the path is not timed; return it as-is
      hdfsModel.uri
    }

    // configure and start streaming
    stream.writeStream
      .format(format)
      .outputMode(mode)
      .options(extraOptions)
      .option("checkpointLocation", checkpointDir)
      .option("path", path)
      .queryName(queryName)
      .start()
  }
}

class HDFSSparkWriter(hdfsModel: RawModel,
                      sc: SparkContext)
  extends SparkWriter with Logging {

  // TODO: validate against hdfsmodel.schema
  override def write(df: DataFrame): Unit = {
    logger.info(s"Initialize the Dataframe HDFS writer: $hdfsModel")

    // calculate path
    val path = if (hdfsModel.timed) {
      // the path must be timed; add timed subdirectory
      val hdfsPath = new Path(hdfsModel.uri)
      val timedPath = new Path(hdfsPath.toString + "/" + ConfigManager.buildTimedName("").substring(1) + "/")

      timedPath.toString
    } else {
      // the path is not timed; return it as-is
      hdfsModel.uri
    }

    // get options
    val options = hdfsModel.options
    val mode = if (options.saveMode == "default") "error" else options.saveMode
    val format = options.format
    val extraOptions = options.extraOptions.getOrElse(Map())

    // setup writer
    val writer = df.write
      .mode(mode)
      .format(format)
      .options(extraOptions)

    logger.info(s"Write in this path: '$path'")

    // write
    writer.save(path)
  }
}