package it.agilelab.bigdata.wasp.consumers.spark.writers

import java.net.URI

import it.agilelab.bigdata.wasp.core.WaspSystem._
import it.agilelab.bigdata.wasp.core.bl.RawBL
import it.agilelab.bigdata.wasp.core.models.RawModel
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.concurrent.Await

object RawWriter {

  def createSparkStreamingWriter(env: {val rawBL: RawBL}, ssc: StreamingContext, id: String): Option[SparkStreamingWriter] = {
    // if we find the model, try to return the correct reader
    val rawModelOpt = getModel(env, id)
    if (rawModelOpt.isDefined) {
      val rawModel = rawModelOpt.get

      // return the correct writer using the uri scheme
      val scheme = new URI(rawModel.uri).getScheme
      scheme match {
        case "hdfs" => Some(new HDFSSparkStreamingWriter(rawModel, ssc))
        case _ => None
      }
    } else {
      None
    }
  }

  def createSparkWriter(env: {val rawBL: RawBL}, sc: SparkContext, id: String): Option[SparkWriter] = {
    // if we find the model, try to return the correct reader
    val rawModelOpt = getModel(env, id)
    if (rawModelOpt.isDefined) {
      val rawModel = rawModelOpt.get

      // return the correct writer using the uri scheme
      val scheme = new URI(rawModel.uri).getScheme
      scheme match {
        case "hdfs" => Some(new HDFSSparkWriter(rawModel, sc))
        case _ => None
      }
    } else {
      None
    }
  }

  private def getModel(env: {val rawBL: RawBL}, id: String): Option[RawModel] = {
    // get the raw model using the provided id & bl
    val rawModelFut = env.rawBL.getById(id)
    Await.result(rawModelFut, timeout.duration)
  }
}

class HDFSSparkStreamingWriter(hdfsModel: RawModel,
                               ssc: StreamingContext)
  extends SparkStreamingWriter {

  override def write(stream: DStream[String]): Unit = {
    // get sql context
    val sqlContext = SQLContext.getOrCreate(ssc.sparkContext)
    val hdfsModelLocal = hdfsModel
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

class HDFSSparkWriter(hdfsModel: RawModel,
                      sc: SparkContext)
  extends SparkWriter {

  // TODO: validate against hdfsmodel.schema
  override def write(df: DataFrame): Unit = {
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

    // write
    writer.save(path)
  }
}