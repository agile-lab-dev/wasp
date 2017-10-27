package it.agilelab.bigdata.wasp.core.utils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.io.DecoderFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{DataType, StructType}

case class AvroToRow(schemaAvroJson: String ) {

  private lazy val userSchema: Schema = new Schema.Parser().parse(schemaAvroJson)
  private lazy  val datumReader = new GenericDatumReader[GenericRecord](userSchema)
  private lazy  val requiredSchema: DataType =  getSchemaSpark()

  private lazy val rowConverter = SchemaConverters.createConverterToSQL(
    userSchema, requiredSchema)
  private lazy val encoderForDataColumns = RowEncoder(requiredSchema.asInstanceOf[StructType])


  def getSchemaSpark(): DataType = SchemaConverters.toSqlType(userSchema).dataType
  def read(avroValue: Array[Byte]): Row = {

    val decoder = DecoderFactory.get.binaryDecoder(avroValue, null)
    val record: GenericRecord = datumReader.read(null, decoder)

    val safeDataRow: GenericRow = rowConverter(record).asInstanceOf[GenericRow]

    safeDataRow
    // The safeDataRow is reused, we must do a copy
    //encoderForDataColumns.toRow(safeDataRow)

  }
}
