package it.agilelab.bigdata.wasp.consumers.spark.plugins.hbase

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkLegacyStreamingWriter, SparkWriter}
import it.agilelab.bigdata.wasp.core.bl.KeyValueBL
import it.agilelab.bigdata.wasp.core.models.{KeyValueModel, ProducerModel}
import it.agilelab.bigdata.wasp.core.utils.RowToAvro
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import spray.json.{DefaultJsonProtocol, JsonParser, RootJsonFormat}
import it.agilelab.bigdata.wasp.core.utils.JsonOps._
import spray.json._





trait HbaseConfigData
case class TableNameC(namespace: String, name: String) extends HbaseConfigData
case class HbaseTableModel(table: TableNameC, rowKey: Seq[RowKeyInfo], columns: Map[String, Seq[Map[String, InfoCol]]]) extends HbaseConfigData
case class RowKeyInfo(col: String, `type`: String) extends HbaseConfigData
case class InfoCol(col: Option[String], `type`: Option[String],  mappingType: String, avro: Option[String], pivotCol: Option[String]) extends HbaseConfigData

object HBaseWriter extends JsonSupport {

	def createSparkStreamingWriter(keyValueBL: KeyValueBL, ssc: StreamingContext, id: String): Option[SparkLegacyStreamingWriter] = {
		// if we find the model, try to return the correct reader
		val hbaseModelOpt = getModel(keyValueBL, id)
		if (hbaseModelOpt.isDefined) {
			val hbaseModel = hbaseModelOpt.get

			Some(new HBaseStreamingWriter(hbaseModel, ssc))
		} else {
			None
		}
	}

	def createSparkWriter(keyValueBL: KeyValueBL, sc: SparkContext, id: String): Option[SparkWriter] = {
		// if we find the model, try to return the correct reader
		val hbaseModelOpt = getModel(keyValueBL, id)
		if (hbaseModelOpt.isDefined) {
			val hbaseModel = hbaseModelOpt.get

			Some(new HBaseWriter(hbaseModel, sc))
		} else {
			None
		}
	}

	private def getModel(keyValueBL: KeyValueBL, id: String): Option[KeyValueModel] = {
		// get the raw model using the provided id & bl
		keyValueBL.getById(id)
	}


	def getHbaseConfDataConvert(json: String): HbaseTableModel = {



		val js: JsValue = json.parseJson
		val result = (js \ "table").field.get.convertTo[TableNameC]
		val rowKey = (js \ "rowkey").field.get.convertTo[Seq[RowKeyInfo]]
		val columns: Map[String, Seq[Map[String, InfoCol]]] = (js \ "columns").field.get.convertTo[Map[String, Seq[Map[String, InfoCol]]]]

		HbaseTableModel(result, rowKey, columns)
	}

	def convertToHBaseType(value: Row, colIdentifier: Int, typeName: String): Array[Byte] = {
		if (!value.isNullAt(colIdentifier)) {
			typeName match {
				case "string" => Bytes.toBytes(value.getAs[String](colIdentifier))
				case "long" => Bytes.toBytes(value.getAs[Long](colIdentifier))
				case "int" => Bytes.toBytes(value.getAs[Int](colIdentifier))
				case "double" => Bytes.toBytes(value.getAs[Double](colIdentifier))
			}
		} else {
			Array[Byte]()
		}
	}
	def getQualifier(qualfier: String, infoCol: InfoCol, r: Row): Array[Byte] = {
		if (infoCol.mappingType == "oneToOne" ) {
			Bytes.toBytes(qualfier)
		} else if (infoCol.mappingType == "oneToMany" && infoCol.pivotCol.isDefined ){
			//TODO Should be possibile to define the type of the pivot column type
			val indexField = r.fieldIndex(infoCol.pivotCol.get)
			val pivotColValue: String = r(indexField).toString
			Array.concat(Bytes.toBytes(qualfier), Bytes.toBytes(pivotColValue))
		} else {
			throw new Exception("mappingType not supportate")
		}

	}
	def getValue(infoCol: InfoCol, r: Row, avroConvertes: Map[String, RowToAvro]): Array[Byte] = {
		if (infoCol.mappingType == "oneToOne" ) {
			val fieldIdentifier = r.fieldIndex(infoCol.col.get)
			convertToHBaseType(r, fieldIdentifier, infoCol.`type`.get)
		} else if (infoCol.mappingType == "oneToMany" && infoCol.pivotCol.isDefined ){
			avroConvertes(infoCol.avro.get).write(r)
		} else {
			throw new Exception("mappingType not supportate")
		}
	}
	def getConvertPutFunc(hbaseModel: HbaseTableModel, avroConvertes: Map[String, RowToAvro]): (Row) => Put = {


		val rowKeyInfo: Seq[RowKeyInfo] = hbaseModel.rowKey
		val columnsInfo = hbaseModel.columns

		(r: Row) => {
			var key = Array[Byte]()
			rowKeyInfo.foreach(v => {
				val fieldIdentifier = r.fieldIndex(v.col)
				if (r.isNullAt(fieldIdentifier)){
					throw new IllegalArgumentException(s"""The field "$fieldIdentifier" is a part of the row key so it cannot be null. $r""")
				}
				key = Array.concat(key, convertToHBaseType(r, fieldIdentifier, v.`type`))
			})
			val putMutation = new Put(key)

			columnsInfo.foreach((cfInfo: (String, Seq[Map[String, InfoCol]])) => {
				val cfByteValue = Bytes.toBytes(cfInfo._1)
					cfInfo._2.foreach((qualifierInfos: Map[String, InfoCol]) => {
						if (qualifierInfos.nonEmpty) {
							val qualifierInfo = qualifierInfos.iterator.next()
							val qualifierByteValue = getQualifier(qualifierInfo._1, qualifierInfo._2, r)
							val valueByte = getValue(qualifierInfo._2, r, avroConvertes)
							putMutation.addColumn(cfByteValue, qualifierByteValue, valueByte)
						}
				})
			})
			putMutation
		}
	}

}
trait HbaseSparkWriter {
	def getHbaseContext(hbaseModel: KeyValueModel, sc: SparkContext): HBaseContext = {
		val options = hbaseModel.options

		// merge additional configuration files if Spark configuration specifies them
		/* TODO verify configuration merging is what we want:
		 * HBaseConfiguration.create() loads from standard files (those in $HBASE_CONF_DIR), which may have configurations
		 * in them that we do not want!
		 * in that case, we should maybe use HBaseConfiguration.create(new Configuration(false)) as base configuration
		 */
		val hBaseConfiguration = HBaseConfiguration.create()
		if (options.isDefined && options.get.contains("core-site") && options.get.contains("hbase-site")) {
			hBaseConfiguration.addResource(new Path(options.get("hbase.configuration.core-site")))
			hBaseConfiguration.addResource(new Path(options.get("hbase.configuration.hbase-site")))
		} else{
			hBaseConfiguration.addResource(new Path("/etc/hbase/conf/core-site.xml"))
			hBaseConfiguration.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))
		}


		new HBaseContext(sc, hBaseConfiguration)
	}
}
class HBaseStreamingWriter(hbaseModel: KeyValueModel,
                               ssc: StreamingContext)
  extends SparkStreamingWriter with HbaseSparkWriter {

	override def write(stream: DStream[String]): Unit = {
		// get sql context
		val sqlContext = SQLContext.getOrCreate(ssc.sparkContext)

		val hBaseContext = getHbaseContext(hbaseModel, ssc.sparkContext)
		//TODO Write a validator of the data converter configurations
		val hbaseDataConfig = HBaseWriter.getHbaseConfDataConvert(hbaseModel.schema)
		val schema: StructType = DataType.fromJson(hbaseModel.dataFrameSchema).asInstanceOf[StructType]

		val avroSchemas = hbaseModel.avroSchemas

		//Validation
		avroSchemas.map(_.mapValues(v => {
			new RowToAvro(schema, v)
		})).getOrElse(Map[String, RowToAvro]())

		val hbaseTable = TableName.valueOf(s"${hbaseDataConfig.table.namespace}:${hbaseDataConfig.table.name}")
		val rowAvroConverters: Map[String, RowToAvro] = avroSchemas.map(_.mapValues(v => {
			new RowToAvro(schema, v)
		})).getOrElse(Map[String, RowToAvro]()).map(identity).toMap


		val conversionFunction: (Row) => Put = HBaseWriter.getConvertPutFunc(hbaseDataConfig, rowAvroConverters)

		hBaseContext.streamBulkPut(stream, hbaseTable, conversionFunction)
	}
}

class HBaseWriter(hbaseModel: KeyValueModel,
                      sc: SparkContext)
	extends SparkWriter with HbaseSparkWriter {
	
	override def write(df: DataFrame): Unit = {
		// get sql context
		val sqlContext = df.sqlContext

		val hBaseContext = getHbaseContext(hbaseModel, sc)
		//TODO Write a validator of the data converter configurations
		val hbaseDataConfig = HBaseWriter.getHbaseConfDataConvert(hbaseModel.schema)
		val hbaseTable = TableName.valueOf(s"${hbaseDataConfig.table.namespace}:${hbaseDataConfig.table.name}")
		val schema: StructType = DataType.fromJson(hbaseModel.dataFrameSchema).asInstanceOf[StructType]

		val rowAvroConverters: Map[String, RowToAvro] = hbaseModel.avroSchemas.map(_.mapValues(v => {
			new RowToAvro(schema, v)
		})).getOrElse(Map[String, RowToAvro]())

		val conversionFunction: (Row) => Put = HBaseWriter.getConvertPutFunc(hbaseDataConfig, rowAvroConverters)
		hBaseContext.bulkPut(df.rdd, hbaseTable, conversionFunction)
	}
}
trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {

	implicit val tableNameCFormat: RootJsonFormat[TableNameC] = jsonFormat2(TableNameC.apply)
	implicit val rowKeyInfoFormat: RootJsonFormat[RowKeyInfo] = jsonFormat2(RowKeyInfo.apply)
	implicit val infoColFormat: RootJsonFormat[InfoCol] = jsonFormat5(InfoCol.apply)


}