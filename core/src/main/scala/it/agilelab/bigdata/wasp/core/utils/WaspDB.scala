package it.agilelab.bigdata.wasp.core.utils

import akka.actor.ActorSystem
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.models.configuration._
import play.api.libs.iteratee.Enumerator
import reactivemongo.api.collections._
import reactivemongo.api.gridfs._
import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.core._
import reactivemongo.api.commands._
import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.collections.bson._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, Future, _}
import scala.reflect.runtime.universe._
import reactivemongo.api.gridfs.Implicits._

import scala.util.{Failure, Success}

trait WaspDB extends MongoDBHelper {

  def getAll[T](implicit typeTag: TypeTag[T], sreader: BSONDocumentReader[T]): Future[List[T]]

  def getDocumentByID[T](id: BSONObjectID)(implicit typeTag: TypeTag[T], sreader: BSONDocumentReader[T]): Future[Option[T]]

  def getDocumentByField[T](field: String, value: BSONValue)(implicit typeTag: TypeTag[T], sreader: BSONDocumentReader[T]): Future[Option[T]]

  def getDocumentByQueryParams[T](query: Map[String, BSONValue])(implicit typeTag: TypeTag[T], sreader: BSONDocumentReader[T]): Future[Option[T]]

  def getAllDocumentsByField[T](field: String, value: BSONValue)(implicit typeTag: TypeTag[T], sreader: BSONDocumentReader[T]): Future[List[T]]

  def insert[T](doc: T)(implicit typeTag: TypeTag[T], swriter: BSONDocumentWriter[T]): Future[WriteResult]

  def insertIfNotExists[T <: Model](doc: T)(implicit typeTag: TypeTag[T], swriter: BSONDocumentWriter[T], sreader: BSONDocumentReader[T]): Future[WriteResult]

  def deleteById[T](id: BSONObjectID)(implicit typeTag: TypeTag[T]): Future[WriteResult]

  def updateById[T](id: BSONObjectID, doc: T)(implicit typeTag: TypeTag[T], swriter: BSONDocumentWriter[T], sreader: BSONDocumentReader[T]): Future[WriteResult]

  def enumerateFile(file: ReadFile[BSONSerializationPack.type, BSONValue]): Enumerator[Array[Byte]]

  def saveFile(arrayBytes: Array[Byte], file: FileToSave[BSONSerializationPack.type, BSONValue]): Future[ReadFile[BSONSerializationPack.type, BSONValue]]

  def deleteFileById(id: BSONObjectID): Future[WriteResult]

  def close(): Unit

  def getFileByID(id: BSONObjectID): Future[Option[ReadFile[BSONSerializationPack.type, BSONValue]]]

}
class WaspDBImp(protected val driver: MongoDriver,
                mongoConnection: MongoConnection,
                databaseName: String) extends WaspDB with BSONConversionHelper  {


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


  private lazy val lookupTable: Map[Type, BSONCollection] = Map(
    typeTag[PipegraphModel].tpe -> collection(pipegraphsName),
    typeTag[ProducerModel].tpe -> collection(producersName),
    typeTag[TopicModel].tpe -> collection(topicsName),
    typeTag[IndexModel].tpe -> collection(indexesName),
    typeTag[RawModel].tpe -> collection(rawName),
    typeTag[KeyValueModel].tpe -> collection(keyValueName),
    typeTag[BatchJobModel].tpe -> collection(batchjobName),
    typeTag[MlModelOnlyInfo].tpe -> collection(mlModelsName),
    typeTag[KafkaConfigModel].tpe -> collection(configurationsName),
    typeTag[SparkBatchConfigModel].tpe -> collection(configurationsName),
    typeTag[SparkStreamingConfigModel].tpe -> collection(configurationsName),
    typeTag[ElasticConfigModel].tpe -> collection(configurationsName),
    typeTag[SolrConfigModel].tpe -> collection(configurationsName),
    typeTag[WebsocketModel].tpe -> collection(websocketsName),
    typeTag[BatchSchedulerModel].tpe -> collection(batchSchedulersName)
  )
  
  private implicit val _db: reactivemongo.api.DefaultDB = mongoConnection(databaseName)


  def collection(collectionName: String) = _db.collection(collectionName, strategy)

  def initializeCollections() {

    def writeCollection(collectionName: String) =
      try {
        Await.result(createCollection(collection(collectionName)), 10.seconds)
      }
      catch {
        case exception: Exception => log.debug("", exception)
      }

    writeCollection(pipegraphsName)
    writeCollection(producersName)
    writeCollection(batchjobName)
    writeCollection(configurationsName)
    writeCollection(websocketsName)
    writeCollection(batchSchedulersName)
  }


  def getAll[T](implicit typeTag: TypeTag[T], sreader: BSONDocumentReader[T]): Future[List[T]] = {
    getAllDocuments[T](lookupTable(typeTag.tpe))
  }

  def getDocumentByID[T](id: BSONObjectID)(implicit typeTag: TypeTag[T], sreader: BSONDocumentReader[T]): Future[Option[T]] = {
    getDocumentByField[T]("_id", id)
  }

  /*def getDocumentByStringField[T](field : String, value : String)(implicit typeTag : TypeTag[T], sreader : BSONDocumentReader[T]) : Future[Option[T]] = {
    getDocumentByField[T](field, BSONString(value))
  }*/

  def getDocumentByField[T](field: String, value: BSONValue)(implicit typeTag: TypeTag[T], sreader: BSONDocumentReader[T]): Future[Option[T]] = {
    getDocumentByKey[T](field, value, lookupTable(typeTag.tpe))
  }

  def getDocumentByQueryParams[T](query: Map[String, BSONValue])(implicit typeTag: TypeTag[T], sreader: BSONDocumentReader[T]): Future[Option[T]] = {
    getDocumentByQueryParams[T](query, lookupTable(typeTag.tpe))
  }

  def getAllDocumentsByField[T](field: String, value: BSONValue)(implicit typeTag: TypeTag[T], sreader: BSONDocumentReader[T]): Future[List[T]] = {
    getAllDocumentsByKey[T](field, value, lookupTable(typeTag.tpe))
  }

  def insert[T](doc: T)(implicit typeTag: TypeTag[T], swriter: BSONDocumentWriter[T]) = {
    addDocumentToCollection(lookupTable(typeTag.tpe), doc)
  }

  def insertIfNotExists[T <: Model](doc: T)(implicit typeTag: TypeTag[T], swriter: BSONDocumentWriter[T], sreader: BSONDocumentReader[T]) = {
    val document = getDocumentByField[T]("name", BSONString(doc.name))
    val result = Await.result(document, 10.seconds)

    result match {
      case Some(value) => future {
        println("Model '" + doc.name + "' already present"); DefaultWriteResult(ok = true, n = 0, writeErrors = Nil, writeConcernError = None,
          code = None,
          errmsg = None)
      }
      case None =>
        println("Model '" + doc.name + "' not found. It will be created.")
        insert(doc)
    }
  }


  def deleteById[T](id: BSONObjectID)(implicit typeTag: TypeTag[T]) = {
    removeDocumentFromCollection("_id", id, lookupTable(typeTag.tpe))
  }

  def updateById[T](id: BSONObjectID, doc: T)(implicit typeTag: TypeTag[T], swriter: BSONDocumentWriter[T], sreader: BSONDocumentReader[T]): Future[WriteResult] = {
    updateDocumentToCollection("_id", id, doc, lookupTable(typeTag.tpe))
  }

  def saveFile(arrayBytes: Array[Byte], file: FileToSave[BSONSerializationPack.type, BSONValue]): Future[ReadFile[BSONSerializationPack.type, BSONValue]] = {
    new GridFS(_db).save(Enumerator(arrayBytes), file)
  }

  def deleteFileById(id: BSONObjectID): Future[WriteResult] = new GridFS(_db).remove(id)

  def enumerateFile(file: ReadFile[BSONSerializationPack.type, BSONValue]): Enumerator[Array[Byte]] = new GridFS(_db).enumerate(file)

  def close() = {
    _db.connection.close()
    driver.close()
  }

  def getFileByID(id: BSONObjectID): Future[Option[ReadFile[BSONSerializationPack.type, BSONValue]]] = {

    log.info(s"Locating file by id $id")
    val query = BSONDocument("_id" -> id)
    new GridFS(_db).find[BSONDocument, ReadFile[BSONSerializationPack.type, BSONValue]](query).headOption
  }
}

object WaspDB {
  private val log = WaspLogger(this.getClass)
  private var waspDB: WaspDB = _

  def apply(driver: MongoDriver, mongoConnection: MongoConnection, databaseName: String): WaspDB = {
    new WaspDBImp(driver, mongoConnection, databaseName)
  }

  def initializeConnectionAndDriver(mongoDBConfig: MongoDBConfigModel, actorSystem: ActorSystem): (MongoDriver, MongoConnection) = {
    val driver = MongoDBHelper.getMongoDriver(actorSystem)
    (driver, MongoDBHelper.getConnection(mongoDBConfig, driver))
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
    val (driver, connection) = initializeConnectionAndDriver(mongoDBConfig, actorSystem)
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
        val completewaspDB = new WaspDBImp(driver, connection, mongoDBConfig.databaseName)
        completewaspDB.initializeCollections()
        waspDB = completewaspDB
/*
      case None => throw new UnknownError("Unknown Error during db initialization")
    }*/

  }


}