package it.agilelab.bigdata.wasp.repository.mongo.utils

import java.util.concurrent.TimeUnit

import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.{Block, ConnectionString}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.configuration.MongoDBConfigModel
import org.mongodb.scala.bson.{BsonBoolean, BsonDocument, BsonDouble, BsonInt32, BsonInt64, BsonString, BsonValue}
import org.mongodb.scala.connection.SocketSettings.Builder
import org.mongodb.scala.connection.SocketSettings
import org.mongodb.scala.result.UpdateResult
import org.mongodb.scala.{MongoClient, MongoClientSettings, MongoDatabase, _}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.postfixOps
import scala.reflect.ClassTag

private[mongo] trait MongoDBHelper extends Logging {

  import MongoDBHelper._

  def mongoDatabase: MongoDatabase

  protected def getCollection(collection: String) =  mongoDatabase.getCollection(collection)

  @com.github.ghik.silencer.silent("deprecated")
  protected def createCollection(collection: String): Unit = {
    try {
      if (!mongoDatabase.listCollectionNames().results().contains(collection)) {
        mongoDatabase
          .createCollection(collection, new CreateCollectionOptions().autoIndex(true))
          .headResult()
      }
    } catch {
      case e: Exception => logger.error("Error during the creation of a collection", e)
    }
  }
  protected def exitsDocumentByKey(key: String, value: BsonValue, collection: String): Boolean = {

    logger.info(s"Locating document(s) by key $key with value $value on collection $collection")
    val query = BsonDocument(key -> value)

    Option(getCollection(collection).find(query).headResult()).isDefined
  }
  protected def getDocumentByKey[T](key: String, value: BsonValue, collection: String)(implicit ct: ClassTag[T]): Option[T] = {

    logger.info(s"Locating document(s) by key $key with value $value on collection $collection")
    val query = BsonDocument(key -> value)

    val document = getCollection(collection).find[T](query)
    val documents = document.results()
    documents.headOption
  }

  protected def getDocumentByQueryParams[T](queryParams: Map[String, BsonValue],sort: Option[BsonDocument], collection: String)(implicit ct: ClassTag[T]): Option[T] = {

    logger.info(s"Locating document(s) by $queryParams on collection $collection with sort: $sort")

    val query = BsonDocument(queryParams)
    val actionBuilder = getCollection(collection).find[T](query)
    sort.map(predicate => actionBuilder.sort(predicate)).getOrElse(actionBuilder).results().headOption
  }

  protected def getAllDocumentsByKey[T](key: String, value: BsonValue, collection: String)(implicit ct: ClassTag[T]): Seq[T] = {

    logger.info(s"Locating document(s) by key $key with value $value on collection $collection")
    val query = BsonDocument(key -> value)

    val document = getCollection(collection).find[T](query)
    document.results()
  }

  protected def getAllDocuments[T](collection: String)(implicit ct: ClassTag[T]): Seq[T] = {

    logger.info(s"Locating document(s) on collection $collection")

    val document = getCollection(collection).find[T]()
    document.results()
  }

  protected def addDocumentToCollection[T](collection: String, doc: T)(implicit ct: ClassTag[T]): Unit = {
    logger.info(s"Adding document to collection $collection")

    try {
      mongoDatabase.getCollection[T](collection).insertOne(doc).results()
      logger.info(s"Document correctly added $doc")
    } catch {
      case e: Exception =>
        logger.error(s"Unable to add document. Error message: ${e.getMessage}")
        throw e
    }
  }

  protected def removeDocumentFromCollection[T: ClassTag](key: String, value: BsonValue, collection: String): Unit = {
    logger.info(s"Removing document from collection $collection")
    val query = BsonDocument(key -> value)

    try {
      val result = getCollection(collection).deleteMany(query).results()
      logger.info(s"Document correctly removed $result, filter: $query")
    } catch {
      case e: Exception =>
        logger.error(s"Unable to delete document. Error message: ${e.getMessage} filter: $query")
        throw e
    }
  }

  protected def removeDocumentFromCollectionByQuery[T: ClassTag](query: BsonDocument, collection: String): Unit = {
    logger.info(s"Removing document from collection $collection")

    try {
      val result = getCollection(collection).deleteMany(query).results()
      logger.info(s"Document correctly removed $result, filter: $query")
    } catch {
      case e: Exception =>
        logger.error(s"Unable to delete document. Error message: ${e.getMessage} filter: $query")
        throw e
    }
  }

  protected def replaceDocumentToCollection[T](key: String, value: BsonValue, updateValue: T, collection: String, upsert:Boolean = false)(implicit ct: ClassTag[T]): UpdateResult = {

    val updateOptions = model.ReplaceOptions().upsert(upsert)

    val selector = BsonDocument(key -> value)
    val result =
      try {
        val result1 = mongoDatabase.getCollection[T](collection).replaceOne(selector, updateValue, updateOptions).headResult()
        logger.info(s"Replaced success for field $key with value $value, updateValue: $updateValue, collection: $collection, result: $result1")
        result1
      } catch {
        case e: Exception =>
          logger.error(s"Unable to replace document. Error message: ${e.getMessage}, field $key with value $value, updateValue: $updateValue, collection: $collection ")
          throw e
      }
    result
  }

}

object MongoDBHelper extends Logging {
  private var resultTimeout = Duration(10, TimeUnit.SECONDS)
  private var mongoClient: MongoClient = _

  implicit class DocumentObservable[C](val observable: Observable[Document]) extends ImplicitObservable[Document] {
    override val converter: (Document) => String = (doc) => doc.toJson
  }

  implicit class GenericObservable[C](val observable: Observable[C]) extends ImplicitObservable[C] {
    override val converter: (C) => String = (doc) => doc.toString
  }

  trait ImplicitObservable[C] {
    val observable: Observable[C]
    val converter: (C) => String

    def results(): Seq[C] = Await.result(observable.toFuture(), resultTimeout)
    def headResult(): C = Await.result(observable.head(), resultTimeout)
    def printResults(initial: String = ""): Unit = {
      if (initial.length > 0) print(initial)
      results().foreach(res => println(converter(res)))
    }
    def printHeadResult(initial: String = ""): Unit = println(s"$initial${converter(headResult())}")
  }


  def close(): Unit = {
    if (mongoClient != null) {
      mongoClient.close()
    }
  }

  @com.github.ghik.silencer.silent("deprecated")
  def getDatabase(mongoDBConfig: MongoDBConfigModel): MongoDatabase = {
    //return a connection pool

    val settingsBuilder = MongoClientSettings.builder()
      .applyConnectionString(new ConnectionString(mongoDBConfig.address))
      //we need full consistency so we consider a write on mongo as successful when it lands on disk
      .writeConcern(WriteConcern.ACKNOWLEDGED.withFsync(true))
      .applyToSocketSettings(new Block[SocketSettings.Builder]{
        override def apply(t: Builder): Unit = t.connectTimeout(mongoDBConfig.millisecondsTimeoutConnection,
          TimeUnit.MILLISECONDS)
          .readTimeout(mongoDBConfig.millisecondsTimeoutConnection,
            TimeUnit.MILLISECONDS)
      })


    val settings = if(mongoDBConfig.username != ""){
      settingsBuilder.credential(MongoCredential.createCredential(
        mongoDBConfig.username,
        mongoDBConfig.credentialDb,
        mongoDBConfig.password.toCharArray)).build()
    } else {
      settingsBuilder.build()
    }

    resultTimeout = Duration(mongoDBConfig.millisecondsTimeoutConnection, TimeUnit.MILLISECONDS)

    mongoClient = MongoClient(settings)
    val mongoDatabase = mongoClient.getDatabase(mongoDBConfig.databaseName)
    // this is done to eagerly initialize connection and fail as early as possible if something is wrong
    mongoDatabase.listCollectionNames().results()
    mongoDatabase
  }

  /**
    * Function to recursively convert a BsonDocument to a Map[String, Any].
    *
    * The keys will be the field names, the values willbe converted to the correpsonding scala types whenever possible.
    *
    * The bson-scala type mappings are as follows:
    * - BsonBoolean   -> Boolean
    * - BsonInt32     -> Int
    * - BsonInt64     -> Long
    * - BsonDouble    -> Double
    * - BsonString    -> String
    * - BsonDocument  -> Map[String, Any]
    * - anything else -> BsonValue
    */
  def bsonDocumentToMap(bsonDocument: BsonDocument): Map[String, Any] = {
    val entries = bsonDocument.entrySet().asScala

    entries map {
      entry =>
        // extract field name to use as key
        val key = entry.getKey

        // extract and convert value to corresponding scala type
        val value = entry.getValue match {
          case boolean: BsonBoolean   => boolean.getValue
          case int: BsonInt32         => int.intValue()
          case long: BsonInt64        => long.longValue()
          case double: BsonDouble     => double.doubleValue()
          case string: BsonString     => string.getValue
          case document: BsonDocument => bsonDocumentToMap(document)
          case x                      => x
        }

        key -> value
    } toMap
  }

  def printMongoConfigModel(mongoDBConfigModel: MongoDBConfigModel): String = {

      s"""address: ${mongoDBConfigModel.address},
         |databaseName: ${mongoDBConfigModel.databaseName},
         |username: ${mongoDBConfigModel.username},
         |password: ************,
         |credentialDb: ${mongoDBConfigModel.credentialDb},
         |millisecondsTimeoutConnection: ${mongoDBConfigModel.millisecondsTimeoutConnection},
         |collectionPrefix: ${mongoDBConfigModel.collectionPrefix}""".stripMargin
  }
}