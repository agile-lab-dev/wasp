package it.agilelab.bigdata.wasp.consumers.spark.plugins.http

import it.agilelab.bigdata.wasp.consumers.spark.utils.CompressExpression
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkStructuredStreamingWriter
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.{HttpCompression, HttpModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types.{ArrayType, BinaryType, MapType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}

class HttpWaspWriter(httpModel: HttpModel) extends SparkStructuredStreamingWriter with Logging {
  private val valColName = "value"
  override def write(stream: DataFrame): DataStreamWriter[Row] = {
    val dfToWrite: DataFrame = prepareDF(stream)

    val writer = HttpWriter(httpModel, valColName)
    dfToWrite
      .writeStream
      .foreach(writer)
  }

  protected[wasp] def prepareDF(stream: DataFrame): DataFrame = {
    val codec = httpModel.compression match {
      case HttpCompression.Disabled => None
      case HttpCompression.Gzip => Some("gzip")
      case HttpCompression.Snappy => throw new IllegalArgumentException("Unsupported compression format: snappy")
      case HttpCompression.Lz4 => throw new IllegalArgumentException("Unsupported compression format: lz4")
    }

    val valueFields = if (httpModel.valueFieldsNames.isEmpty) {
      httpModel.headersFieldName.fold(stream.schema.fieldNames)(h => stream.schema.fieldNames.filterNot(_ == h)).toList
    } else {
      httpModel.valueFieldsNames
    }

    val conf = stream.sparkSession.sparkContext.hadoopConfiguration

    val valueColumn = (valueFields match {
      case head :: Nil =>
        stream.schema.fields.find(_.name == head).map(_.dataType) match {
          case Some(_: MapType) | Some(_: ArrayType) =>
            if (httpModel.structured) to_json(struct(head)) else to_json(col(head))
          case Some(_: StructType) => to_json(col(head))
          case None =>
            throw new IllegalArgumentException(
              s"Cannot find column $head inside data frame, columns are " + stream.schema.fields.mkString("[", ",", "]")
            )
          case _ => to_json(struct(head))
        }
      case _ =>
        to_json(struct(valueFields.map(col): _*))
    }).cast(BinaryType)

    val compressionF = codec.map(c => CompressExpression.compress(valueColumn, c, conf)).getOrElse(valueColumn)

    val headerColumn = httpModel.headersFieldName.map { hName =>
      val hField = stream.schema.fields.find(_.name == hName)
        .getOrElse(throw new RuntimeException(s"Cannot find header column: $hName"))
      hField.dataType match {
        case MapType(StringType, StringType, _) => col(hName)
        case MapType(keyType, valueType, _) =>
          logger.warn(s"header column $hName is not of type Map[String, String] but it is " +
            s"Map[$keyType, $valueType] a cast will be performed")
          col(hName).cast(MapType(StringType, StringType)).as(hName)
        case ArrayType(elements@StructType(Array(_, _)), _) =>
          logger.warn(s"header column $hName is not of type Map[String, String] but it is " +
            s"Array[$elements] a cast will be performed")
          map_from_entries(col(hName)).cast(MapType(StringType, StringType)).as(hName)
        case headerDataType =>
          throw new RuntimeException(s"column $hName is of type $headerDataType which cannot " +
            s"be used as http headers")
      }
    }.toSeq

    val dfToWrite = stream
      .select(headerColumn :+ compressionF.as(valColName): _*)
    dfToWrite
  }
}
