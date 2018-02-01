package it.agilelab.bigdata.wasp.consumers.spark.plugins.raw

import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkLegacyStreamingWriter, SparkStructuredStreamingWriter, SparkWriter}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.RawModel
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

class RawSparkLegacyStreamingWriter(hdfsModel: RawModel,
                                    ssc: StreamingContext)
  extends SparkLegacyStreamingWriter with Logging {

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
          val partitionBy = options.partitionBy.getOrElse(Nil)
  
          // setup writer
          val writer = df.write
            .mode(mode)
            .format(format)
            .options(extraOptions)
            .partitionBy(partitionBy:_*)

          // write
          writer.save(path)
        }
    }
  }
}

class RawSparkStructuredStreamingWriter(hdfsModel: RawModel,
                                        ss: SparkSession)
  extends SparkStructuredStreamingWriter with Logging {

  override def write(stream: DataFrame, queryName: String, checkpointDir: String): Unit = {
  
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
    
    // get other options
    val options = hdfsModel.options
    val mode = if (options.saveMode == "default") "append" else hdfsModel.options.saveMode
    val format = options.format
    val extraOptions = options.extraOptions.getOrElse(Map())
    val partitionBy = options.partitionBy.getOrElse(Nil)

    // configure and start streaming
    stream.writeStream
      .format(format)
      .outputMode(mode)
      .options(extraOptions)
      .partitionBy(partitionBy:_*)
      .option("checkpointLocation", checkpointDir)
      .option("path", path)
      .queryName(queryName)
      .start()
  }
}

class RawSparkWriter(hdfsModel: RawModel,
                     sc: SparkContext)
  extends SparkWriter
    with Logging {

  // TODO: validate against hdfsmodel.schema
  override def write(df: DataFrame): Unit = {
    logger.info(s"Initializing HDFS writer: $hdfsModel")

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
    val partitionBy = options.partitionBy.getOrElse(Nil)

    // setup writer
    val writer = df.write
      .mode(mode)
      .format(format)
      .options(extraOptions)
      .partitionBy(partitionBy:_*)
  
    logger.info(s"Write in this path: '$path'")

    // write
    writer.save(path)
  }
}

object RawWriters {
  /** Prepares a DataFrame with duplicated columns; useful for when wiriting with partitionBy and then reading a single
    * subdirectory without losing data from the partitionBY columns.
    *
    * Returns a new DataFrame with duplicates of the columns in `columns` and the list of new column names. The new names
    * are created by prepending "_".
    *
    * @param df start datafame
    * @param columns columns to duplicate
    * @return a tuple (newDataFrame, newColumnNames)
    */
  def duplicateColumns(df: DataFrame, columns: String*): (DataFrame, Seq[String]) = {
    // generate new column names and add duplicate columns to df
    val (dfWithAllNewColumns, allNewColumns) = columns.foldLeft((df, List.empty[String])) {
      case ((dfWithNewColumns, newColumns), column) => {
        val newColumn = "_" + column
        val dfWithNewColumn = dfWithNewColumns.withColumn(newColumn, col(column))
        
        (dfWithNewColumn, newColumn :: newColumns)
      }
    }
    
    // return new df & new column names (we need to reverse this list because we were prepending them)
    (dfWithAllNewColumns, allNewColumns.reverse)
  }
}