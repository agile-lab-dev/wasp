package it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration

import it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration.sink.HBaseSink
import org.apache.hadoop.classification.InterfaceAudience
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{Admin, ColumnFamilyDescriptorBuilder, TableDescriptorBuilder}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

import java.io.{File, IOException, ObjectInputStream, ObjectOutputStream}
import scala.util.control.NonFatal

@InterfaceAudience.Private
class DefaultSource extends RelationProvider
  with DataSourceRegister
  with CreatableRelationProvider
  with StreamSinkProvider
  with Logging {


  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    HBaseRelation(parameters)(sqlContext)
  }

  override def shortName(): String = "hbase"

  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    throw new NotImplementedError("This plugin does not support spark batch")
  }

  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {

    if (outputMode != OutputMode.Append()) {
      log.error("Append is the only supported OutputMode for HBase.")
      throw new IllegalArgumentException("Append is only supported OutputMode for HBase. " +
        s"Cannot continue with [$outputMode].")
    }

    HBaseRelation(parameters)(sqlContext).createTable()

    val sparkSession = sqlContext.sparkSession

    this.synchronized {
      if (LatestHBaseContextCache.latest == null) {
        val config = HBaseConfiguration.create()
        val configResources = parameters.getOrElse(HBaseSparkConf.HBASE_CONFIG_LOCATION, "")
        configResources.split(",").foreach(r => config.addResource(r))
        configResources.split(",")
          .filter(r => (r != "") && new File(r).exists())
          .foreach(r => config.addResource(new Path(r)))
        new HBaseContext(sparkSession.sparkContext, config)
      }
    }

    new HBaseSink(sparkSession, parameters, LatestHBaseContextCache.latest)
  }
}

@InterfaceAudience.Private
case class HBaseRelation(
  @transient parameters: Map[String, String]
)(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan with InsertableRelation with Logging {

  val catalog: HBaseTableCatalog = HBaseTableCatalog(parameters)

  def tableName: String = catalog.namespace + ":" + catalog.tableName

  private val configResources: String = parameters.getOrElse(HBaseSparkConf.HBASE_CONFIG_LOCATION, "")
  private val useHBaseContext: Boolean = parameters.get(HBaseSparkConf.USE_HBASECONTEXT)
    .map(_.toBoolean)
    .getOrElse(HBaseSparkConf.DEFAULT_USE_HBASECONTEXT)

  //create or get latest HBaseContext
  private val hbaseContext: HBaseContext = if (useHBaseContext) {
    LatestHBaseContextCache.latest
  } else {
    val config = HBaseConfiguration.create()
    configResources.split(",").filter(r => (r != "") && new File(r).exists()).foreach(r => {
      log.info(s"HBase configuration file: $r")
      config.addResource(new Path(r))
    })
    log.info(s"HBase configurations $config")
    new HBaseContext(sqlContext.sparkContext, config)
  }

  private val wrappedConf = new SerializableConfiguration(hbaseContext.config)

  def hbaseConf: Configuration = wrappedConf.value

  /**
   * Generates a Spark SQL schema objeparametersct so Spark SQL knows what is being
   * provided by this BaseRelation
   *
   * @return schema generated from the SCHEMA_COLUMNS_MAPPING_KEY value
   */
  override val schema: StructType = HBaseTableCatalog.schema


  def createTable(): Unit = {
    val numReg = parameters.get(HBaseTableCatalog.newTable).map(x => x.toInt).getOrElse(0)
    val startKey = Bytes.toBytes(parameters.getOrElse(HBaseTableCatalog.regionStart,
      HBaseTableCatalog.defaultRegionStart))
    val endKey = Bytes.toBytes(parameters.getOrElse(HBaseTableCatalog.regionEnd, HBaseTableCatalog.defaultRegionEnd))

    if (numReg > 3) {
      val tName = catalog.namespace match {
        case Some(namespace) => TableName.valueOf(namespace, catalog.tableName)
        case None => TableName.valueOf(catalog.tableName)
      }

      val connection = HBaseConnectionCache.getConnection(hbaseConf)
      val admin = connection.getAdmin

      try {
        if (!admin.isTableAvailable(tName)) {
          log.info("Creating table {}", tName)
          val tableDescBuilder = TableDescriptorBuilder.newBuilder(tName)
          catalog.columnFamilies.foreach { x =>
            val descriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(x))
            tableDescBuilder.setColumnFamily(descriptor.build())
            logDebug(s"add family $x to $tableName")
          }
          val splitKeys = Bytes.split(startKey, endKey, numReg);
          admin.createTable(tableDescBuilder.build(), splitKeys)
        }
      } finally {
        admin.close()
        connection.close()
      }
    } else {
      logInfo(s"${HBaseTableCatalog.newTable} is not defined or no larger than 3, skip the create table")
    }
  }

  def createNamespaceIfNotExist(connection: Admin, namespace: String): Boolean = {
    try {
      connection
        .listNamespaceDescriptors()
        .map(_.getName)
        .contains(namespace)
    } catch {
      case _: Exception => false
    }
  }

  class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
    def writeObject(out: ObjectOutputStream): Unit = tryOrIOException {
      out.defaultWriteObject()
      value.write(out)
    }

    def readObject(in: ObjectInputStream): Unit = tryOrIOException {
      value = new Configuration(false)
      value.readFields(in)
    }

    def tryOrIOException(block: => Unit): Unit = {
      try {
        block
      } catch {
        case e: IOException => throw e
        case NonFatal(t) => throw new IOException(t)
      }
    }
  }

  override def buildScan(requiredColumns: Array[String],
                         filters: Array[Filter]): RDD[Row] = {
    throw new NotImplementedError("This plugin does not support read operation of any kind!")
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    throw new NotImplementedError("This plugin does not support batch writes")
  }
}
