package it.agilelab.bigdata.wasp.consumers.spark.writers

import it.agilelab.bigdata.wasp.core.bl.KeyValueBL
import it.agilelab.bigdata.wasp.core.models.KeyValueModel
import it.agilelab.bigdata.wasp.core.utils.JsonOps._
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, RowToAvro}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import spray.json.{DefaultJsonProtocol, JsonParser, RootJsonFormat}
import it.agilelab.bigdata.wasp.core.utils.JsonOps._


trait HbaseConfigData

case class TableNameC(namespace: String, name: String) extends HbaseConfigData

case class HbaseTableModel(table: TableNameC, rowKey: Seq[RowKeyInfo], columns: Map[String, Seq[Map[String, InfoCol]]]) extends HbaseConfigData

case class RowKeyInfo(col: String, `type`: String) extends HbaseConfigData

case class InfoCol(col: Option[String], `type`: Option[String], mappingType: String, avro: Option[String], pivotCol: Option[String]) extends HbaseConfigData

/**
  * TODO WARN MUST BE TESTED!!!!
  */
object HBaseWriter {

  def createSparkStreamingWriter(env: {val keyValueBL: KeyValueBL}, ssc: StreamingContext, id: String): Option[SparkStreamingWriter] = {
    // if we find the model, try to return the correct reader
    val hbaseModelOpt = getModel(env, id)
    if (hbaseModelOpt.isDefined) {
      val hbaseModel = hbaseModelOpt.get

      Some(new HBaseStreamingWriter(hbaseModel, ssc))
    } else {
      None
    }
  }

  def createSparkWriter(env: {val keyValueBL: KeyValueBL}, sc: SparkContext, id: String): Option[SparkWriter] = {
    // if we find the model, try to return the correct reader
    val hbaseModelOpt = getModel(env, id)
    if (hbaseModelOpt.isDefined) {
      val hbaseModel = hbaseModelOpt.get

      Some(new HBaseWriter(hbaseModel, sc))
    } else {
      None
    }
  }

  private def getModel(env: {val keyValueBL: KeyValueBL}, id: String): Option[KeyValueModel] = {
    // get the raw model using the provided id & bl
    env.keyValueBL.getById(id)
  }


  def getHbaseConfDataConvert(json: String): HbaseTableModel = {
    import DefaultJsonProtocol._
    implicit val tableNameCFormat: RootJsonFormat[TableNameC] = jsonFormat2(TableNameC.apply)
    implicit val rowKeyInfoFormat: RootJsonFormat[RowKeyInfo] = jsonFormat2(RowKeyInfo.apply)
    implicit val infoColFormat: RootJsonFormat[InfoCol] = jsonFormat5(InfoCol.apply)

    val js = JsonParser(json)
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
    if (infoCol.mappingType == "oneToOne") {
      Bytes.toBytes(qualfier)
    } else if (infoCol.mappingType == "oneToMany" && infoCol.pivotCol.isDefined) {
      //TODO Should be possibile to define the type of the pivot column type
      val indexField = r.fieldIndex(infoCol.pivotCol.get)
      val pivotColValue: String = r(indexField).toString
      Array.concat(Bytes.toBytes(qualfier), Bytes.toBytes(pivotColValue))
    } else {
      throw new Exception("mappingType not supportate")
    }

  }

  def getValue(infoCol: InfoCol, r: Row, avroConvertes: Map[String, RowToAvro]): Array[Byte] = {
    if (infoCol.mappingType == "oneToOne") {
      val fieldIdentifier = r.fieldIndex(infoCol.col.get)
      convertToHBaseType(r, fieldIdentifier, infoCol.`type`.get)
    } else if (infoCol.mappingType == "oneToMany" && infoCol.pivotCol.isDefined) {
      avroConvertes(infoCol.avro.get).write(r)
    } else {
      throw new Exception("mappingType not supportate")
    }
  }

  def getConvertPutFunc(hbaseModel: HbaseTableModel, avroConvertes: Map[String, RowToAvro]): (Row) => (ImmutableBytesWritable, Put) = {


    val rowKeyInfo: Seq[RowKeyInfo] = hbaseModel.rowKey
    val columnsInfo = hbaseModel.columns

    (r: Row) => {
      var key = Array[Byte]()
      rowKeyInfo.foreach(v => {
        val fieldIdentifier = r.fieldIndex(v.col)
        if (r.isNullAt(fieldIdentifier)) {
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
      (new ImmutableBytesWritable(key), putMutation)
    }
  }

}

class HBaseStreamingWriter(hbaseModel: KeyValueModel,
                           ssc: StreamingContext)
  extends SparkStreamingWriter {

  override def write(stream: DStream[String]): Unit = {
    // get sql context
    val sqlContext = SQLContext.getOrCreate(ssc.sparkContext)

    // create hbase configuration
    val hBaseConfiguration = HBaseConfiguration.create()
    hBaseConfiguration.addResource(new Path(ConfigManager.getHBaseConfig.coreSiteXmlPath))
    hBaseConfiguration.addResource(new Path(ConfigManager.getHBaseConfig.hbaseSiteXmlPath))


    //TODO Write a validator of the data converter configurations
    val hbaseDataConfig = HBaseWriter.getHbaseConfDataConvert(hbaseModel.schema)
    val schema: StructType = DataType.fromJson(hbaseModel.dataFrameSchema).asInstanceOf[StructType]

    val avroSchemas = hbaseModel.avroSchemas

    val jobConfig: JobConf = new JobConf(hBaseConfiguration, this.getClass)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, s"${hbaseDataConfig.table.namespace}:${hbaseDataConfig.table.name}")


    //Validation
    avroSchemas.map(_.mapValues(v => {
      new RowToAvro(schema, v)
    })).getOrElse(Map[String, RowToAvro]())

    stream.foreachRDD {
      rdd =>
        val rowAvroConverters: Map[String, RowToAvro] = avroSchemas.map(_.mapValues(v => {
          new RowToAvro(schema, v)
        })).getOrElse(Map[String, RowToAvro]()).map(identity).toMap

        val hbaseTable = TableName.valueOf(s"${hbaseDataConfig.table.namespace}:${hbaseDataConfig.table.name}")
        // create df from rdd using provided schema & spark's json datasource
        val df: DataFrame = sqlContext.read.schema(schema).json(rdd)

        val conversionFunction: (Row) => (ImmutableBytesWritable, Put) = HBaseWriter.getConvertPutFunc(hbaseDataConfig, rowAvroConverters)
        df.rdd.map(r => conversionFunction(r)).saveAsNewAPIHadoopDataset(jobConfig)
    }
  }
}

class HBaseWriter(hbaseModel: KeyValueModel,
                  sc: SparkContext)
  extends SparkWriter {

  override def write(df: DataFrame): Unit = {
    // get sql context
    val sqlContext = df.sqlContext

    // prepare hbase configuration
    val hBaseConfiguration = HBaseConfiguration.create()
    // merge additional configuration files if configuration specifies them
    /* TODO verify configuration merging is what we want:
     * HBaseConfiguration.create() loads from standard files (those in $HBASE_CONF_DIR), which may have configurations
     * in them that we do not want!
     * in that case, we should maybe use HBaseConfiguration.create(new Configuration(false)) as base configuration
     */
    hBaseConfiguration.addResource(new Path(ConfigManager.getHBaseConfig.coreSiteXmlPath))
    hBaseConfiguration.addResource(new Path(ConfigManager.getHBaseConfig.hbaseSiteXmlPath))


    //TODO Write a validator of the data converter configurations
    val hbaseDataConfig = HBaseWriter.getHbaseConfDataConvert(hbaseModel.schema)
    val schema: StructType = DataType.fromJson(hbaseModel.dataFrameSchema).asInstanceOf[StructType]

    val rowAvroConverters: Map[String, RowToAvro] = hbaseModel.avroSchemas.map(_.mapValues(v => {
      new RowToAvro(schema, v)
    })).getOrElse(Map[String, RowToAvro]())


    val jobConfig: JobConf = new JobConf(hBaseConfiguration, this.getClass)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, s"${hbaseDataConfig.table.namespace}:${hbaseDataConfig.table.name}")

    val conversionFunction: (Row) => (ImmutableBytesWritable, Put) = HBaseWriter.getConvertPutFunc(hbaseDataConfig, rowAvroConverters)

    df.rdd.map(r => conversionFunction(r)).saveAsNewAPIHadoopDataset(jobConfig)

  }
}