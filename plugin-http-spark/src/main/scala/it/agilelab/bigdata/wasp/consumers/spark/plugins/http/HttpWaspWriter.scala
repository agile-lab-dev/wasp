package it.agilelab.bigdata.wasp.consumers.spark.plugins.http

import it.agilelab.bigdata.wasp.consumers.spark.utils.CompressExpression
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkStructuredStreamingWriter
import it.agilelab.bigdata.wasp.models.{HttpCompression, HttpModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types.{ArrayType, BinaryType, MapType, StructType}
import org.apache.spark.sql.{DataFrame, Row}

class HttpWaspWriter(httpModel: HttpModel) extends SparkStructuredStreamingWriter {
  private val valColName = "value"
  override def write(stream: DataFrame): DataStreamWriter[Row] = {
    val writer = HttpWriter(httpModel, valColName)
    val codec = httpModel.compression match {
      case HttpCompression.Disabled => None
      case HttpCompression.Gzip     => Some("gzip")
      case HttpCompression.Snappy   => throw new IllegalArgumentException("Unsupported compression format: snappy")
      case HttpCompression.Lz4      => throw new IllegalArgumentException("Unsupported compression format: lz4")
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

    stream
      .select(httpModel.headersFieldName.toSeq.map(col) :+ compressionF.as(valColName): _*)
      .writeStream
      .foreach(writer)
  }

}
