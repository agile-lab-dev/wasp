package it.agilelab.bigdata.wasp.core.utils

import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.file.{Paths, StandardOpenOption}

import com.mongodb.async.client.gridfs.helpers.{AsynchronousChannelHelper => JAsynchronousChannelHelper}
import org.mongodb.scala.gridfs.{AsyncInputStream, GridFSBucket, GridFSUploadOptions, GridFSUploadStream}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.models.configuration._
import it.agilelab.bigdata.wasp.core.utils.MongoDBHelper._
import org.bson.codecs.configuration.CodecProvider
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.types.ObjectId
import org.mongodb.scala.{Completed, MongoDatabase}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.bson.{BsonDocument, BsonObjectId, BsonString, BsonValue}
import org.mongodb.scala.result.UpdateResult
import org.mongodb.scala.gridfs.GridFSBucket

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

trait WaspDB extends MongoDBHelper {

  def getAll[T]()(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Seq[T]

  def getDocumentByID[T](id: BsonObjectId)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[T]

  def getDocumentByField[T](field: String, value: BsonValue)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[T]

  def getDocumentByQueryParams[T](query: Map[String, BsonValue])(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[T]

  def getAllDocumentsByField[T](field: String, value: BsonValue)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Seq[T]

  def getAllRaw[T]()(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Seq[BsonDocument]

  def getDocumentByIDRaw[T](id: BsonObjectId)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[BsonDocument]

  def getDocumentByFieldRaw[T](field: String, value: BsonValue)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[BsonDocument]

  def getDocumentByQueryParamsRaw[T](query: Map[String, BsonValue])(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[BsonDocument]

  def getAllDocumentsByFieldRaw[T](field: String, value: BsonValue)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Seq[BsonDocument]

  def insert[T](doc: T)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Unit

  def insertIfNotExists[T <: Model](doc: T)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Unit

  def deleteById[T](id: BsonObjectId)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Unit

  def updateById[T](id: BsonObjectId, doc: T)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): UpdateResult

  def saveFile(arrayBytes: Array[Byte], file: String, metadata: BsonDocument): BsonObjectId

  def deleteFileById(id: BsonObjectId): Unit

  def close(): Unit

  def getFileByID(id: BsonObjectId): Array[Byte]

}
class WaspDBImp(val mongoDatabase: MongoDatabase) extends WaspDB   {
  import WaspDB._

  def initializeCollections() {
    createCollection(pipegraphsName)
    createCollection(producersName)
    createCollection(rawName)
    createCollection(batchjobName)
    createCollection(configurationsName)
    createCollection(websocketsName)
    createCollection(batchSchedulersName)
    createCollection(mlModelsName)
  }


  def getAll[T]()(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Seq[T] = {
    getAllDocuments[T](lookupTable(typeTag.tpe))
  }

  def getDocumentByID[T](id: BsonObjectId)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[T] = {
    getDocumentByField[T]("_id", id)
  }


  def getDocumentByField[T](field: String, value: BsonValue)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[T] = {
    getDocumentByKey[T](field, value, lookupTable(typeTag.tpe))
  }

  def getDocumentByQueryParams[T](query: Map[String, BsonValue])(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[T] = {
    getDocumentByQueryParams[T](query, lookupTable(typeTag.tpe))
  }

  def getAllDocumentsByField[T](field: String, value: BsonValue)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Seq[T] = {
    getAllDocumentsByKey[T](field, value, lookupTable(typeTag.tpe))
  }

  def getAllRaw[T]()(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Seq[BsonDocument] = {
    getAllDocuments[BsonDocument](lookupTable(typeTag.tpe))
  }

  def getDocumentByIDRaw[T](id: BsonObjectId)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[BsonDocument] = {
    getDocumentByFieldRaw[T]("_id", id)
  }


  def getDocumentByFieldRaw[T](field: String, value: BsonValue)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[BsonDocument] = {
    getDocumentByKey[BsonDocument](field, value, lookupTable(typeTag.tpe))
  }

  def getDocumentByQueryParamsRaw[T](query: Map[String, BsonValue])(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[BsonDocument] = {
    getDocumentByQueryParams[BsonDocument](query, lookupTable(typeTag.tpe))
  }

  def getAllDocumentsByFieldRaw[T](field: String, value: BsonValue)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Seq[BsonDocument] = {
    getAllDocumentsByKey[BsonDocument](field, value, lookupTable(typeTag.tpe))
  }

  def insert[T](doc: T)(implicit ct: ClassTag[T], typeTag: TypeTag[T]) = {
    addDocumentToCollection(lookupTable(typeTag.tpe), doc)
  }

  def insertIfNotExists[T <: Model](doc: T)(implicit ct: ClassTag[T], typeTag: TypeTag[T]) = {
    val exits = exitsDocumentByKey("name", BsonString(doc.name), lookupTable(typeTag.tpe))
    if (exits) {
      logger.info("Model '" + doc.name + "' already present")
    } else {
      logger.info("Model '" + doc.name + "' not found. It will be created.")
      insert(doc)
    }
    Unit
  }
  
  def deleteById[T](id: BsonObjectId)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Unit = {
    removeDocumentFromCollection[T]("_id", id, lookupTable(typeTag.tpe))
  }

  def updateById[T](id: BsonObjectId, doc: T)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): UpdateResult = {
    replaceDocumentToCollection[T]("_id", id, doc, lookupTable(typeTag.tpe))
  }

  def saveFile(arrayBytes: Array[Byte], file: String, metadata: BsonDocument): BsonObjectId = {
    val uploadStreamFile = GridFSBucket(mongoDatabase).openUploadStream(file)
    uploadStreamFile.write(ByteBuffer.wrap(arrayBytes)).subscribe(
      (x: Int) => None, (throwable: Throwable ) => (), () => {
        uploadStreamFile.close().subscribe(
          (x: Completed) => None, (throwable: Throwable ) => (), () => {
          })
      })

    BsonObjectId(uploadStreamFile.objectId)
  }

  def deleteFileById(id: BsonObjectId): Unit = GridFSBucket(mongoDatabase).delete(id).headResult()

  def enumerateFile(file: String): Array[Byte] = {
    val gridFile = GridFSBucket(mongoDatabase).openDownloadStream(file)
    val length = gridFile.gridFSFile().headResult().getLength
    // MUST be less than 4GB
    assert(length < Integer.MAX_VALUE)
    val resultFile = ByteBuffer.allocate(length.toInt)
    gridFile.read(resultFile)
    resultFile.array()

  }

  def close() = {
    MongoDBHelper.close()
  }

  def getFileByID(id: BsonObjectId): Array[Byte] = {

    logger.info(s"Locating file by id $id")
    val gridFile = GridFSBucket(mongoDatabase).openDownloadStream(id)
    val length = gridFile.gridFSFile().headResult().getLength
    // MUST be less than 4GB
    assert(length < Integer.MAX_VALUE)
    val resultFile = ByteBuffer.allocate(length.toInt)
    gridFile.read(resultFile).headResult()
    resultFile.array()
  }
}

object WaspDB extends Logging {
  private var waspDB: WaspDB = _


  val pipegraphsName = "pipegraphs"
  val producersName = "producers"
  val topicsName = "topics"
  val indexesName = "indexes"
  val rawName = "raw"
  val keyValueName = "keyvalues"
  val batchjobName = "batchjobs"
  val configurationsName = "configurations"
  val mlModelsName = "mlmodels"
  val websocketsName = "websockets"
  val batchSchedulersName = "batchschedulers"


  val lookupTable: Map[Type, String] = Map(
    typeTag[PipegraphModel].tpe ->  pipegraphsName,
    typeTag[ProducerModel].tpe -> producersName,
    typeTag[TopicModel].tpe -> topicsName,
    typeTag[IndexModel].tpe -> indexesName,
    typeTag[RawModel].tpe -> rawName,
    typeTag[KeyValueModel].tpe -> keyValueName,
    typeTag[BatchJobModel].tpe -> batchjobName,
    typeTag[MlModelOnlyInfo].tpe -> mlModelsName,
    typeTag[KafkaConfigModel].tpe -> configurationsName,
    typeTag[SparkBatchConfigModel].tpe -> configurationsName,
    typeTag[SparkStreamingConfigModel].tpe -> configurationsName,
    typeTag[ElasticConfigModel].tpe -> configurationsName,
    typeTag[SolrConfigModel].tpe -> configurationsName,
    typeTag[HBaseConfigModel].tpe -> configurationsName,
    typeTag[WebsocketModel].tpe -> websocketsName,
    typeTag[BatchSchedulerModel].tpe -> batchSchedulersName
  )


  private lazy val codecRegisters: java.util.List[CodecProvider] = List(
	  createCodecProviderIgnoreNone(classOf[ConnectionConfig]),
	  createCodecProviderIgnoreNone(classOf[DashboardModel]),
	  createCodecProviderIgnoreNone(classOf[RTModel]),
	  createCodecProviderIgnoreNone(classOf[LegacyStreamingETLModel]),
    createCodecProviderIgnoreNone(classOf[StructuredStreamingETLModel]),
	  createCodecProviderIgnoreNone(classOf[PipegraphModel]),
	  createCodecProviderIgnoreNone(classOf[ProducerModel]),
	  createCodecProviderIgnoreNone(classOf[ReaderType]),
	  createCodecProviderIgnoreNone(classOf[ReaderModel]),
	  createCodecProviderIgnoreNone(classOf[MlModelOnlyInfo]),
	  createCodecProviderIgnoreNone(classOf[StrategyModel]),
	  createCodecProviderIgnoreNone(classOf[WriterType]),
	  createCodecProviderIgnoreNone(classOf[WriterModel]),
	  createCodecProviderIgnoreNone(classOf[TopicModel]),
	  createCodecProviderIgnoreNone(classOf[IndexModel]),
	  createCodecProviderIgnoreNone(classOf[RawOptions]),
	  createCodecProviderIgnoreNone(classOf[RawModel]),
	  createCodecProviderIgnoreNone(classOf[KeyValueModel]),
	  createCodecProviderIgnoreNone(classOf[BatchJobModel]),
	  createCodecProviderIgnoreNone(classOf[KafkaConfigModel]),
	  createCodecProviderIgnoreNone(classOf[SparkBatchConfigModel]),
	  createCodecProviderIgnoreNone(classOf[SparkStreamingConfigModel]),
	  createCodecProviderIgnoreNone(classOf[ElasticConfigModel]),
	  createCodecProviderIgnoreNone(classOf[SolrConfigModel]),
	  createCodecProviderIgnoreNone(classOf[HBaseConfigModel]),
	  createCodecProviderIgnoreNone(classOf[WebsocketModel]),
	  createCodecProviderIgnoreNone(classOf[BatchSchedulerModel])
  ).asJava


  def initializeConnectionAndDriver(mongoDBConfig: MongoDBConfigModel): MongoDatabase = {
    val mongoDatabase = MongoDBHelper.getDatabase(mongoDBConfig)

    mongoDatabase
  }
  
  def getDB: WaspDB = {
    if (waspDB == null) {
      val msg = "The waspDB was not initialized"
      logger.error(msg)
      throw new Exception(msg)
    }
    waspDB
  }

  def initializeDB(): Unit = {
    // MongoDB initialization
    val mongoDBConfig = ConfigManager.getMongoDBConfig
    logger.info(s"Create connection to MongoDB: address ${mongoDBConfig.address}, databaseName: ${mongoDBConfig.databaseName}")

    val codecRegistry = fromRegistries(fromProviders(codecRegisters), DEFAULT_CODEC_REGISTRY)

    val mongoDBDatabase = initializeConnectionAndDriver(mongoDBConfig).withCodecRegistry(codecRegistry)
    /*val primaryNode = connection.wait()
    val primaryNodeReady = Await.ready(primaryNode, 6.second)
    println(primaryNodeReady.value)

    primaryNodeReady.value match {
      case Some(Failure(t)) =>
        val msg = s"There is no MongoDB instance active on address ${mongoDBConfig.address}, databaseName: ${mongoDBConfig.databaseName}. Message: ${t.getMessage}"
        log.error(msg)
        connection.close()
        driver.close()
        throw new Exception(msg)

      case Some(Success(_)) =>*/
        val completewaspDB = new WaspDBImp(mongoDBDatabase)
        completewaspDB.initializeCollections()
        waspDB = completewaspDB

    /*
          case None => throw new UnknownError("Unknown Error during db initialization")
        }*/

  }


}