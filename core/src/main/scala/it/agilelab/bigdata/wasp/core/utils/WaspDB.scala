package it.agilelab.bigdata.wasp.core.utils

import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.file.{Paths, StandardOpenOption}

import com.mongodb.ErrorCategory
import com.mongodb.async.client.Observables
import com.mongodb.async.client.gridfs.helpers.{AsynchronousChannelHelper => JAsynchronousChannelHelper}
import com.mongodb.client.model.{CreateCollectionOptions, IndexOptions}
import org.mongodb.scala.gridfs.{AsyncInputStream, GridFSBucket, GridFSUploadOptions, GridFSUploadStream}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.models.configuration._
import it.agilelab.bigdata.wasp.core.utils.MongoDBHelper._
import org.bson.codecs.configuration.CodecProvider
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.types.ObjectId
import org.mongodb.scala.{Completed, MongoCommandException, MongoDatabase, MongoWriteException}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.bson.{BsonDocument, BsonObjectId, BsonString, BsonValue}
import org.mongodb.scala.result.UpdateResult
import org.mongodb.scala.gridfs.GridFSBucket

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

trait WaspDB extends MongoDBHelper {

  def upsert[T <: Model](doc: T)(implicit ct: ClassTag[T], typeTag: TypeTag[T])

  def getAll[T <: Model]()(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Seq[T]

  def getDocumentByField[T <: Model](field: String, value: BsonValue)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[T]

  def getDocumentByQueryParams[T <: Model](query: Map[String, BsonValue])(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[T]

  def getAllDocumentsByField[T <: Model](field: String, value: BsonValue)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Seq[T]

  def getAllRaw[T <: Model]()(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Seq[BsonDocument]

  def getDocumentByFieldRaw[T <: Model](field: String, value: BsonValue)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[BsonDocument]

  def getDocumentByQueryParamsRaw[T <: Model](query: Map[String, BsonValue])(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[BsonDocument]

  def getAllDocumentsByFieldRaw[T <: Model](field: String, value: BsonValue)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Seq[BsonDocument]

  def insert[T <: Model](doc: T)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Unit

  def insertIfNotExists[T <: Model](doc: T)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Unit

  def deleteByName[T <: Model](name: String)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Unit

  def updateByName[T <: Model](name: String, doc: T)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): UpdateResult

  def saveFile(arrayBytes: Array[Byte], file: String, metadata: BsonDocument): BsonObjectId

  def deleteFileById(id: BsonObjectId): Unit

  def close(): Unit

  def getFileByID(id: BsonObjectId): Array[Byte]

}
class WaspDBImp(val mongoDatabase: MongoDatabase) extends WaspDB   {
  import WaspDB._

  /**
    * initializes collections.
    *
    * Collections are initialized concurrently by different nodes so each node tries to create it and backs off
    * if another node concurrently created the collections.
    *
    * To force name as key of models an index with unique constraint is concurrently created, if another node concurrently
    * created the index the current node backs off.
    */
  def initializeCollections() {

    import org.mongodb.scala.model.Indexes._

    val collections = lookupTable.values.toSet

    val collectionOptions = new CreateCollectionOptions().autoIndex(true)


    val COLLECTION_ALREADY_EXISTS = 48

    val results = collections.map( collection => (collection, Try(mongoDatabase.createCollection(collection, collectionOptions).results())))
                             .map {
                               //everything is fine
                               case (collectionName:String, Success(_)) => Right(collectionName)
                               //collection already exist, nothing to do
                               case (collectionName:String, Failure(ex: MongoCommandException))
                                 if ex.getErrorCode == COLLECTION_ALREADY_EXISTS => Right(collectionName)
                               //collection correctly created
                               case (_, Failure(ex: MongoCommandException))
                                 if ex.getErrorCode != COLLECTION_ALREADY_EXISTS => Left(ex)
                             }

    val failures = results.filter(_.isLeft)

    if(failures.nonEmpty){
      val message = failures.map(_.left.get)
                            .map(_.toString)
                            .mkString(System.lineSeparator())

      throw new Exception(message)
    }


    val createdCollections = results.filter(_.isRight).map(_.right.get)


    val indexOptions = new IndexOptions().unique(true)

    val INDEX_ALREADY_EXISTS = 68

    val indexResults = createdCollections.map(collectionName => (collectionName, Try(mongoDatabase.getCollection(collectionName)
                                                                                                  .createIndex(ascending("name"), indexOptions)
                                                                                                  .results())))
                                         .map {
                                           //everything is fine
                                           case (collectionName:String, Success(_)) => Right(collectionName)
                                           //collection already exist, nothing to do
                                           case (collectionName:String, Failure(ex: MongoCommandException))
                                             if ex.getErrorCode == INDEX_ALREADY_EXISTS => Right(collectionName)
                                           //collection correctly created
                                           case (_, Failure(ex: MongoCommandException))
                                             if ex.getErrorCode != INDEX_ALREADY_EXISTS => Left(ex)
                                         }


    val indexFailures = indexResults.filter(_.isLeft)

    if(indexFailures.nonEmpty) {
        val message = indexFailures.map(_.left.get)
                                   .map(_.toString)
                                   .mkString(System.lineSeparator())

        throw new Exception(message)
    }

  }


  def getAll[T <: Model]()(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Seq[T] = {
    getAllDocuments[T](lookupTable(typeTag.tpe))
  }

  def getDocumentByField[T <: Model](field: String, value: BsonValue)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[T] = {
    getDocumentByKey[T](field, value, lookupTable(typeTag.tpe))
  }

  def getDocumentByQueryParams[T <: Model](query: Map[String, BsonValue])(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[T] = {
    getDocumentByQueryParams[T](query, lookupTable(typeTag.tpe))
  }

  def getAllDocumentsByField[T <: Model](field: String, value: BsonValue)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Seq[T] = {
    getAllDocumentsByKey[T](field, value, lookupTable(typeTag.tpe))
  }

  def getAllRaw[T <: Model]()(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Seq[BsonDocument] = {
    getAllDocuments[BsonDocument](lookupTable(typeTag.tpe))
  }

  def getDocumentByFieldRaw[T <: Model](field: String, value: BsonValue)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[BsonDocument] = {
    getDocumentByKey[BsonDocument](field, value, lookupTable(typeTag.tpe))
  }

  def getDocumentByQueryParamsRaw[T <: Model](query: Map[String, BsonValue])(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[BsonDocument] = {
    getDocumentByQueryParams[BsonDocument](query, lookupTable(typeTag.tpe))
  }

  def getAllDocumentsByFieldRaw[T <: Model](field: String, value: BsonValue)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Seq[BsonDocument] = {
    getAllDocumentsByKey[BsonDocument](field, value, lookupTable(typeTag.tpe))
  }

  def insert[T <: Model](doc: T)(implicit ct: ClassTag[T], typeTag: TypeTag[T]) = {
    addDocumentToCollection(lookupTable(typeTag.tpe), doc)
  }

  def upsert[T <: Model](doc: T)(implicit ct: ClassTag[T], typeTag: TypeTag[T]) = {

    logger.info(s"Upserting model '${doc.name}'")

    replaceDocumentToCollection("name", BsonString(doc.name),doc, lookupTable(typeTag.tpe), upsert=true)

    Unit
  }

  def insertIfNotExists[T <: Model](doc: T)(implicit ct: ClassTag[T], typeTag: TypeTag[T]) = {

    try {
      mongoDatabase.getCollection[T](lookupTable(typeTag.tpe)).insertOne(doc).results()
    } catch {
      case ex : MongoWriteException if ex.getError.getCategory == ErrorCategory.DUPLICATE_KEY  => logger.info("document already present, doing nothing")
    }

    Unit
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

  override def deleteByName[T <: Model](name: String)(implicit ct: ClassTag[T], typeTag: universe.TypeTag[T]): Unit =
    removeDocumentFromCollection[T]("name", BsonString(name), lookupTable(typeTag.tpe))

  override def updateByName[T <: Model](name: String, doc: T)(implicit ct: ClassTag[T], typeTag: universe.TypeTag[T]): UpdateResult =
    replaceDocumentToCollection[T]("name", BsonString(name), doc, lookupTable(typeTag.tpe))

}

object WaspDB extends Logging {
  private var waspDB: WaspDB = _


  val pipegraphsName = "pipegraphs"
  val producersName = "producers"
  val topicsName = "topics"
  val indexesName = "indexes"
  val rawName = "raw"
  val keyValueName = "keyvalues"
  val sqlSourceName = "sqlsource"
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
    typeTag[SqlSourceModel].tpe -> sqlSourceName,
    typeTag[BatchJobModel].tpe -> batchjobName,
    typeTag[MlModelOnlyInfo].tpe -> mlModelsName,
    typeTag[WebsocketModel].tpe -> websocketsName,
    typeTag[BatchSchedulerModel].tpe -> batchSchedulersName,

    typeTag[KafkaConfigModel].tpe -> configurationsName,
    typeTag[SparkBatchConfigModel].tpe -> configurationsName,
    typeTag[SparkStreamingConfigModel].tpe -> configurationsName,
    typeTag[ElasticConfigModel].tpe -> configurationsName,
    typeTag[SolrConfigModel].tpe -> configurationsName,
    typeTag[SolrConfigModel].tpe -> configurationsName,
    typeTag[HBaseConfigModel].tpe -> configurationsName,
    typeTag[JdbcConfigModel].tpe -> configurationsName
  )


  private lazy val codecRegisters: java.util.List[CodecProvider] = List(
	  createCodecProviderIgnoreNone(classOf[ConnectionConfig]),
	  createCodecProviderIgnoreNone(classOf[ZookeeperConnection]),
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
	  createCodecProviderIgnoreNone(classOf[KeyValueOption]),
	  createCodecProviderIgnoreNone(classOf[KeyValueModel]),
    createCodecProviderIgnoreNone(classOf[SqlSourceModel]),
	  createCodecProviderIgnoreNone(classOf[BatchETLModel]),
	  createCodecProviderIgnoreNone(classOf[BatchJobModel]),
	  createCodecProviderIgnoreNone(classOf[KafkaConfigModel]),
	  createCodecProviderIgnoreNone(classOf[SparkBatchConfigModel]),
	  createCodecProviderIgnoreNone(classOf[SparkStreamingConfigModel]),
	  createCodecProviderIgnoreNone(classOf[ElasticConfigModel]),
	  createCodecProviderIgnoreNone(classOf[SolrConfigModel]),
	  createCodecProviderIgnoreNone(classOf[HBaseConfigModel]),
    createCodecProviderIgnoreNone(classOf[JdbcConfigModel]),
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