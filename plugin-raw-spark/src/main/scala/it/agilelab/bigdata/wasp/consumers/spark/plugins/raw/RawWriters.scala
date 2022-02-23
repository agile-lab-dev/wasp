package it.agilelab.bigdata.wasp.consumers.spark.plugins.raw

import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.hdfs.HdfsUtils
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkBatchWriter, SparkStructuredStreamingWriter}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.RawModel
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

class RawSparkStructuredStreamingWriter(hdfsModel: RawModel, ss: SparkSession)
    extends SparkStructuredStreamingWriter
    with Logging {

  override def write(stream: DataFrame): DataStreamWriter[Row] = {

    // get path timed or standard
    val path = HdfsUtils.getRawModelPathToWrite(hdfsModel)

    // get other options
    val options      = hdfsModel.options
    val mode         = if (options.saveMode == "default") "append" else hdfsModel.options.saveMode
    val format       = options.format
    val extraOptions = options.extraOptions.getOrElse(Map())
    val partitionBy  = options.partitionBy.getOrElse(Nil)

    // create and configure DataStreamWriter
    stream.writeStream
      .format(format)
      .outputMode(mode)
      .options(extraOptions)
      .partitionBy(partitionBy: _*)
      .option("path", path)
  }
}

class RawSparkBatchWriter(hdfsModel: RawModel, sc: SparkContext) extends SparkBatchWriter with Logging {

  // TODO: validate against hdfsmodel.schema
  override def write(df: DataFrame): Unit = {
    logger.info(s"Initializing HDFS writer: $hdfsModel")

    // calculate path
    val path = HdfsUtils.getRawModelPathToWrite(hdfsModel)

    // get options
    val options      = hdfsModel.options
    val mode         = if (options.saveMode == "default") "error" else options.saveMode
    val format       = options.format
    val extraOptions = options.extraOptions.getOrElse(Map())
    val partitionBy  = options.partitionBy.getOrElse(Nil)

    // setup writer
    val writer = df.write
      .mode(mode)
      .format(format)
      .options(extraOptions)
      .partitionBy(partitionBy: _*)

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
        val newColumn       = "_" + column
        val dfWithNewColumn = dfWithNewColumns.withColumn(newColumn, col(column))

        (dfWithNewColumn, newColumn :: newColumns)
      }
    }

    // return new df & new column names (we need to reverse this list because we were prepending them)
    (dfWithAllNewColumns, allNewColumns.reverse)
  }
}
