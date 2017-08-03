package it.agilelab.bigdata.wasp.core.utils

import java.util.concurrent.TimeUnit

import com.mongodb.ConnectionString
import com.mongodb.client.model.CreateCollectionOptions
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.models.configuration.MongoDBConfigModel
import org.mongodb.scala.bson.{BsonDocument, BsonValue}
import org.mongodb.scala.connection.{ClusterSettings, SocketSettings}
import org.mongodb.scala.result.UpdateResult
import org.mongodb.scala.{MongoClient, MongoClientSettings, MongoDatabase, _}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

private[utils] trait MongoDBHelper {

  import MongoDBHelper._

  //protected var queryMaxWaitTime = 2500;
  protected val log = WaspLogger(this.getClass)

  protected def mongoDatabase: MongoDatabase

  protected def getCollection(collection: String) =  mongoDatabase.getCollection(collection)
  protected def createCollection(collection: String): Unit = {
      mongoDatabase
        .createCollection(collection, new CreateCollectionOptions().autoIndex(true))
        .headResult()
  }

  protected def getDocumentByKey[T](key: String, value: BsonValue, collection: String)(implicit ct: ClassTag[T]): Option[T] = {

    log.info(s"Locating document(s) by key $key with value $value on collection $collection")
    val query = BsonDocument(key -> value)

    getCollection(collection).find[T](query).results().headOption
  }

  protected def getDocumentByQueryParams[T](queryParams: Map[String, BsonValue], collection: String)(implicit ct: ClassTag[T]): Option[T] = {

    log.info(s"Locating document(s) by $queryParams on collection $collection")

    val query = BsonDocument(queryParams)

    getCollection(collection).find[T](query).results().headOption
  }

  protected def getAllDocumentsByKey[T](key: String, value: BsonValue, collection: String)(implicit ct: ClassTag[T]): Seq[T] = {

    log.info(s"Locating document(s) by key $key with value $value on collection $collection")
    val query = BsonDocument(key -> value)

    getCollection(collection).find[T](query).results()
  }

  protected def getAllDocuments[T](collection: String)(implicit ct: ClassTag[T]): Seq[T] = {

    log.info(s"Locating document(s) on collection $collection")

    getCollection(collection).find[T]().results()
  }

  protected def addDocumentToCollection[T](collection: String, doc: T)(implicit ct: ClassTag[T]): Unit = {
    log.info(s"Adding document to collection $collection")

    try {
      mongoDatabase.getCollection[T](collection).insertOne(doc).results()
      log.info(s"Document correctly added $doc")
    } catch {
      case e: Exception =>
        log.error(s"Unable to add document. Error message: ${e.getMessage}")
        throw e
    }
  }

  protected def removeDocumentFromCollection[T](key: String, value: BsonValue, collection: String)(implicit ct: ClassTag[T]): Unit = {
    log.info(s"Removing document from collection $collection")
    val query = BsonDocument(key -> value)


    try {
      val result = getCollection(collection).deleteMany(query).results()
      log.info(s"Document correctly removed $result, filter: $query")
    } catch {
      case e: Exception =>
        log.error(s"Unable to delete document. Error message: ${e.getMessage} filter: $query")
        throw e
    }
  }

  protected def replaceDocumentToCollection[T](key: String, value: BsonValue, updateValue: T, collection: String)(implicit ct: ClassTag[T]): UpdateResult = {

    val selector = BsonDocument(key -> value)
    val result =
      try {
      val result1 = mongoDatabase.getCollection[T](collection).replaceOne(selector, updateValue).headResult()
      log.info(s"Replaced success for field $key with value $value, updateValue: $updateValue, collection: $collection, result: $result1")
      result1
    } catch {
      case e: Exception =>
        log.error(s"Unable to delete document. Error message: ${e.getMessage}, field $key with value $value, updateValue: $updateValue, collection: $collection ")
        throw e
    }
    result
  }

}

object MongoDBHelper {
  private val log = WaspLogger(this.getClass)
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
  def getDatabase(mongoDBConfig: MongoDBConfigModel): MongoDatabase = {
    //return a connection pool
    val clusterSettings: ClusterSettings = ClusterSettings.builder()
      .applyConnectionString(new ConnectionString(mongoDBConfig.address))
      .build()
    val settings =
      MongoClientSettings.builder().clusterSettings(clusterSettings).heartbeatSocketSettings(
        SocketSettings.builder().
          connectTimeout(mongoDBConfig.secondsTimeoutConnection, TimeUnit.SECONDS)
          .readTimeout(mongoDBConfig.secondsTimeoutConnection, TimeUnit.SECONDS)
          .build())
        .build()

    resultTimeout =   Duration(mongoDBConfig.secondsTimeoutConnection, TimeUnit.SECONDS)

    mongoClient = MongoClient(settings)
    val mongoDatabase = mongoClient.getDatabase(mongoDBConfig.databaseName)

    mongoDatabase.listCollectionNames().results()
    mongoDatabase
  }
}