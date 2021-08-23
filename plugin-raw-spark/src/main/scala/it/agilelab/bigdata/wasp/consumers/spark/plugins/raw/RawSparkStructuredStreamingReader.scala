package it.agilelab.bigdata.wasp.consumers.spark.plugins.raw

import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkStructuredStreamingReader
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.hdfs.HdfsUtils
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.{RawModel, StreamingReaderModel, StructuredStreamingETLModel}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class RawSparkStructuredStreamingReader(rawModel: RawModel) extends SparkStructuredStreamingReader with Logging {
  /**
    * Create a streaming DataFrame from a streaming source.
    *
    * @param streamingReaderModel the model for the streamign source from which the stream originates
    * @param ss                   the Spark Session to use
    * @return
    */
  override def createStructuredStream(etl: StructuredStreamingETLModel, streamingReaderModel: StreamingReaderModel)(implicit ss: SparkSession): DataFrame = {
    require(etl.streamingInput.datastoreModelName == rawModel.name, s"etl model input and rawModel do not match: " +
      s"etl.streamingInput.name=[${etl.streamingInput.datastoreModelName}], rawModel.name=[${rawModel.name}]")

    logger.info(s"Initialize Spark Raw Reader with this model: $rawModel")

    // setup reader
    val schema: Option[StructType] = if (rawModel.schema.isEmpty) {
      None
    } else {
      Some(DataType.fromJson(rawModel.schema).asInstanceOf[StructType])
    }
    val options            = rawModel.options
    val extraOptions       =
      options.extraOptions.getOrElse(Map()) ++ etl.options ++ streamingReaderModel.options
    val reader = schema.fold(ss.readStream)(ss.readStream.schema(_))
      .format(options.format)
      .options(extraOptions)

    // calculate path
    val path = HdfsUtils.getRawModelPathToToLoad(rawModel, ss.sparkContext)

    logger.info(s"Load this path: '$path'")
    logger.info(RawSparkReaderUtils.printExtraOptions(rawModel.name, extraOptions))
    reader.load(path)
  }
}
