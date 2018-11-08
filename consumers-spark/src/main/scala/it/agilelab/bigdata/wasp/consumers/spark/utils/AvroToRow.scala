package it.agilelab.bigdata.wasp.consumers.spark.utils

import com.typesafe.config.Config
import it.agilelab.darwin.manager.AvroSchemaManager
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.DataType

import scala.collection.mutable


case class AvroToRow(schemaAvroJson: String,
                     useAvroSchemaManager: Boolean,
                     avroSchemaManagerConfig: Option[Config]) {

  require(!useAvroSchemaManager || useAvroSchemaManager && avroSchemaManagerConfig.isDefined,
    "if useAvroSchemaManager is true avroSchemaManagerConfig must have a value")

  val mapDatumReader: mutable.Map[Long, GenericDatumReader[GenericRecord]] = mutable.Map.empty

  private lazy val datumReader = new GenericDatumReader[GenericRecord](userSchema)
  private lazy val requiredSchema: DataType = getSchemaSpark()

  private lazy val rowConverter = SchemaConverters.createConverterToSQL(userSchema, requiredSchema)

  //private lazy val encoderForDataColumns = RowEncoder(requiredSchema.asInstanceOf[StructType])
  private def userSchema: Schema = new Schema.Parser().parse(schemaAvroJson)

  def getSchemaSpark(): DataType = SchemaConverters.toSqlType(userSchema).dataType

  /** Deserialize Avro.
    * If use [[AvroSchemaManager]]. First 2 byte is a MAGIC NUMBER (C3 01), 8 byte is a hash of [[Schema]].
    *
    */
  def read(avroValue: Array[Byte]): Row = {


    val reader = useAvroSchemaManager match {

      case true =>
        AvroSchemaManager.instance(avroSchemaManagerConfig.get)

        AvroSchemaManager.isAvroSingleObjectEncoded(avroValue) match {

          case true => foundGenericDatumReaderInMap(avroValue)

          case false => (avroValue, datumReader)
        }

      case false => (avroValue, datumReader)
    }


    val decoder = DecoderFactory.get.binaryDecoder(reader._1, null)
    val record: GenericRecord = reader._2.read(null, decoder)

    val safeDataRow: GenericRow = rowConverter(record).asInstanceOf[GenericRow]

    safeDataRow
    // The safeDataRow is reused, we must do a copy
    //encoderForDataColumns.toRow(safeDataRow)
  }


  def foundGenericDatumReaderInMap(avroValue: Array[Byte]): (Array[Byte], GenericDatumReader[GenericRecord]) = {

    //AvroSchemaManager.instance(avroSchemaManagerConfig) NO need here since it is called at line 44

    val schemaAndAvro: (Schema, Array[Byte]) = AvroSchemaManager.retrieveSchemaAndAvroPayload(avroValue)

    val schemaId: Long = AvroSchemaManager.getId(schemaAndAvro._1)
    val datum: Option[GenericDatumReader[GenericRecord]] = mapDatumReader.get(schemaId)

    if (datum.isDefined) {
      (schemaAndAvro._2, datum.get)
    }

    else {
      val newdatum: GenericDatumReader[GenericRecord] = new GenericDatumReader[GenericRecord](schemaAndAvro._1, userSchema)
      mapDatumReader += (schemaId -> newdatum)
      (schemaAndAvro._2, newdatum)
    }
  }
}

object AvroToRow
