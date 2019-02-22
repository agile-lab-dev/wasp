package it.agilelab.bigdata.wasp.consumers.spark.utils

import com.typesafe.config.Config
import it.agilelab.darwin.manager.util.AvroSingleObjectEncodingUtils
import it.agilelab.darwin.manager.{AvroSchemaManager, AvroSchemaManagerFactory}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.DataType

import scala.collection.concurrent.TrieMap


case class AvroToRow(schemaAvroJson: String,
                     useAvroSchemaManager: Boolean,
                     avroSchemaManagerConfig: Option[Config]) {

  require(!useAvroSchemaManager || useAvroSchemaManager && avroSchemaManagerConfig.isDefined,
    "if useAvroSchemaManager is true avroSchemaManagerConfig must have a value")

  // this is None if useAvroSchemaManager is false, even if the config has a value, so in the following code we do not
  // need to check both
  @transient
  lazy val darwin = if (useAvroSchemaManager) {
    Some(AvroSchemaManagerFactory.initialize(avroSchemaManagerConfig.get))
  } else {
    None
  }

  val mapDatumReader: TrieMap[Long, GenericDatumReader[GenericRecord]] = TrieMap.empty

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

    val reader = darwin.map { schemaManager: AvroSchemaManager =>
      if (AvroSingleObjectEncodingUtils.isAvroSingleObjectEncoded(avroValue)) {
        foundGenericDatumReaderInMap(avroValue, schemaManager)
      } else {
        (avroValue, datumReader)
      }
    }.getOrElse((avroValue, datumReader))

    val decoder = DecoderFactory.get.binaryDecoder(reader._1, null)
    val record: GenericRecord = reader._2.read(null, decoder)

    val safeDataRow: GenericRow = rowConverter(record).asInstanceOf[GenericRow]

    safeDataRow
    // The safeDataRow is reused, we must do a copy
    //encoderForDataColumns.toRow(safeDataRow)
  }


  def foundGenericDatumReaderInMap(fullPayload: Array[Byte],
                                   schemaManager: AvroSchemaManager): (Array[Byte], GenericDatumReader[GenericRecord]) = {
    val (schema, avro) = schemaManager.retrieveSchemaAndAvroPayload(fullPayload)

    val schemaId: Long = schemaManager.getId(schema)

    avro -> mapDatumReader.getOrElseUpdate(schemaId, new GenericDatumReader[GenericRecord](schema, userSchema))

  }
}
