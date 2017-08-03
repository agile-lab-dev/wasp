package it.agilelab.bigdata.wasp.core.utils

import java.nio.ByteBuffer

import akka.actor.ActorSystem
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.models.configuration._
import it.agilelab.bigdata.wasp.core.utils.MongoDBHelper._
import org.mongodb.scala.bson.{BsonDocument, BsonObjectId, BsonString, BsonValue}
import org.mongodb.scala.gridfs.GridFSBucket
import org.mongodb.scala.result.UpdateResult
import org.mongodb.scala.{Document, MongoDatabase}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

trait WaspDB extends MongoDBHelper {

  def getAll[T]()(implicit ct: ClassTag[T]): Seq[T]

  def getDocumentByID[T](id: BsonObjectId)(implicit ct: ClassTag[T]): Option[T]

  def getDocumentByField[T](field: String, value: BsonValue)(implicit ct: ClassTag[T]): Option[T]

  def getDocumentByQueryParams[T](query: Map[String, BsonValue])(implicit ct: ClassTag[T]): Option[T]

  def getAllDocumentsByField[T](field: String, value: BsonValue)(implicit ct: ClassTag[T]): Seq[T]

  def insert[T](doc: T)(implicit ct: ClassTag[T]): Unit

  def insertIfNotExists[T <: Model](doc: T)(implicit ct: ClassTag[T]): Unit

  def deleteById[T](id: BsonObjectId)(implicit ct: ClassTag[T]): Unit

  def updateById[T](id: BsonObjectId, doc: T)(implicit ct: ClassTag[T]): UpdateResult

  def saveFile(arrayBytes: Array[Byte], file: String, metadata: BsonDocument): BsonObjectId

  def deleteFileById(id: BsonObjectId): Unit

  def close(): Unit

  def getFileByID(id: BsonObjectId): Array[Byte]

}
class WaspDBImp(protected val mongoDatabase: MongoDatabase) extends WaspDB   {


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


  private lazy val lookupTable: Map[Type, String] = Map(
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
    typeTag[WebsocketModel].tpe -> websocketsName,
    typeTag[BatchSchedulerModel].tpe -> batchSchedulersName
  )
  

  def initializeCollections() {
    createCollection(pipegraphsName)
    createCollection(producersName)
    createCollection(batchjobName)
    createCollection(configurationsName)
    createCollection(websocketsName)
    createCollection(batchSchedulersName)
  }


  def getAll[T]()(implicit ct: ClassTag[T]): Seq[T] = {
    getAllDocuments[T](lookupTable(typeTag.tpe))
  }

  def getDocumentByID[T](id: BsonObjectId)(implicit ct: ClassTag[T]): Option[T] = {
    getDocumentByField[T]("_id", id)
  }


  def getDocumentByField[T](field: String, value: BsonValue)(implicit ct: ClassTag[T]): Option[T] = {
    getDocumentByKey[T](field, value, lookupTable(typeTag.tpe))
  }

  def getDocumentByQueryParams[T](query: Map[String, BsonValue])(implicit ct: ClassTag[T]): Option[T] = {
    getDocumentByQueryParams[T](query, lookupTable(typeTag.tpe))
  }

  def getAllDocumentsByField[T](field: String, value: BsonValue)(implicit ct: ClassTag[T]): Seq[T] = {
    getAllDocumentsByKey[T](field, value, lookupTable(typeTag.tpe))
  }

  def insert[T](doc: T)(implicit ct: ClassTag[T]) = {
    addDocumentToCollection(lookupTable(typeTag.tpe), doc)
  }

  def insertIfNotExists[T <: Model](doc: T)(implicit ct: ClassTag[T]) = {
    val document = getDocumentByField[T]("name", BsonString(doc.name))

    document match {
      case Some(_) =>
        log.info("Model '" + doc.name + "' already present");
      case None =>
        log.info("Model '" + doc.name + "' not found. It will be created.")
        insert(doc)
    }
    Unit
  }


  def deleteById[T](id: BsonObjectId)(implicit ct: ClassTag[T]): Unit = {
    removeDocumentFromCollection[T]("_id", id, lookupTable(typeTag.tpe))
  }

  def updateById[T](id: BsonObjectId, doc: T)(implicit ct: ClassTag[T]): UpdateResult = {
    replaceDocumentToCollection[T]("_id", id, doc, lookupTable(typeTag.tpe))
  }

  def saveFile(arrayBytes: Array[Byte], file: String, metadata: BsonDocument): BsonObjectId = {
    val uploadStreamFile = GridFSBucket(mongoDatabase)
      .openUploadStream(file)
    uploadStreamFile.write(ByteBuffer.wrap(arrayBytes))
    uploadStreamFile.close()
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

    log.info(s"Locating file by id $id")
    val gridFile = GridFSBucket(mongoDatabase).openDownloadStream(id)
    val length = gridFile.gridFSFile().headResult().getLength
    // MUST be less than 4GB
    assert(length < Integer.MAX_VALUE)
    val resultFile = ByteBuffer.allocate(length.toInt)
    gridFile.read(resultFile)
    resultFile.array()
  }
}

object WaspDB {
  private val log = WaspLogger(this.getClass)
  private var waspDB: WaspDB = _


  def initializeConnectionAndDriver(mongoDBConfig: MongoDBConfigModel, actorSystem: ActorSystem): MongoDatabase = {
     val mongoDatabase = MongoDBHelper.getDatabase(mongoDBConfig)
    mongoDatabase.listCollectionNames().results()
    mongoDatabase

  }
  
  def getDB: WaspDB = {
    if (waspDB == null) {
      val msg = "The waspDB was not initialized"
      log.error(msg)
      throw new Exception(msg)
    }
    waspDB
  }

  def DBInitialization(actorSystem: ActorSystem): Unit = {
    // MongoDB initialization
    val mongoDBConfig = ConfigManager.getMongoDBConfig
    log.info(s"Create connection to MongoDB: address ${mongoDBConfig.address}, databaseName: ${mongoDBConfig.databaseName}")
    assert(actorSystem != null)
    val mongoDBDatabase = initializeConnectionAndDriver(mongoDBConfig, actorSystem)
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