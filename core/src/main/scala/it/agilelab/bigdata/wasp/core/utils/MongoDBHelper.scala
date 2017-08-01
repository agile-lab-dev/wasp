package it.agilelab.bigdata.wasp.core.utils

import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.models.configuration.MongoDBConfigModel

import akka.actor.ActorSystem
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}
import reactivemongo.api._
import reactivemongo.api.collections._
import reactivemongo.bson._
import reactivemongo.api.collections.bson._
import scala.reflect.runtime.universe._
import reactivemongo.api.commands._

private[utils] trait MongoDBHelper {



  //protected var queryMaxWaitTime = 2500;
  protected val log = WaspLogger(this.getClass)
  protected def driver: MongoDriver
  protected val strategy =
    FailoverStrategy(
      initialDelay = new FiniteDuration(500, java.util.concurrent.TimeUnit.MILLISECONDS),
      retries = 3,
      delayFactor = attemptNumber => 1 + attemptNumber * 0.5
    )

  implicit object BSONValueStringReader extends BSONReader[BSONValue, String] {
    def read(v: BSONValue) = v match {
      case oid: BSONObjectID => oid.stringify
      case BSONString(i) => i.toString
      case BSONInteger(i) => i.toString
      case BSONLong(l) => l.toString
      case BSONDouble(d) => d.toString
      case BSONBoolean(b) => b.toString
      case BSONDateTime(dt) => dt.toString
      // ... and so on 
    }
  }

  protected def createCollection(collection: BSONCollection): Future[Unit] = {

    val future = collection.create(autoIndexId = true)

    future.onComplete {
      case Success(p) =>
      case Failure(e) =>
        println(s"collection ${collection.name} has been already defined")
    }

    future
  }

  protected def getDocumentByKey[T](key: String, value: BSONValue, collection: BSONCollection)(implicit sreader: BSONDocumentReader[T]): Future[Option[T]] = {

    log.info(s"Locating document(s) by key $key with value $value on collection ${collection.name}")
    val query = BSONDocument(key -> value)

    collection.find(query).one[T] //.collect[List](1, true).map(e => e.headOption)
  }

  protected def getDocumentByQueryParams[T](queryParams: Map[String, BSONValue], collection: BSONCollection)(implicit sreader: BSONDocumentReader[T]): Future[Option[T]] = {

    log.info(s"Locating document(s) by $queryParams on collection ${collection.name}")

    val query = BSONDocument(queryParams)

    collection.find(query).one[T] //.collect[List](1, true).map(e => e.headOption)
  }

  protected def getAllDocumentsByKey[T](key: String, value: BSONValue, collection: BSONCollection)(implicit sreader: BSONDocumentReader[T]): Future[List[T]] = {

    log.info(s"Locating document(s) by key $key with value $value on collection ${collection.name}")
    val query = BSONDocument(key -> value)

    collection.find(query).cursor[T].collect[List](Int.MaxValue, stopOnError = false) //TODO: How many docs shall we give back?
  }

  protected def getAllDocuments[T](collection: BSONCollection)(implicit sreader: BSONDocumentReader[T]): Future[List[T]] = {

    log.info(s"Locating document(s) on collection ${collection.name}")
    val query = BSONDocument()

    collection.find(query).cursor[T].collect[List](Int.MaxValue, stopOnError = false) //TODO: How many docs shall we give back?
  }

  protected def addDocumentToCollection[T](collection: BSONCollection, doc: T)(implicit swriter: BSONDocumentWriter[T]) = {
    log.info(s"Adding document to collection ${collection.name}")
    val future: Future[WriteResult] = collection.insert(doc)

    future.onComplete {
      case Failure(e) => log.info(s"Unable to add document. Error message: ${e.getMessage}")
      case Success(_) => log.info(s"Document correctly added")
    }

    future
  }

  protected def removeDocumentFromCollection[T](key: String, value: BSONValue, collection: BSONCollection) = {
    log.info(s"Removing document from collection ${collection.name}")
    val query = BSONDocument(key -> value)
    val futureRemove: Future[WriteResult] = collection.remove(query)

    futureRemove.onComplete {
      case Failure(e) => log.info(s"Unable to delete document. Error message: ${e.getMessage}")
      case Success(_) => log.info(s"Document correctly removed")
    }

    futureRemove
  }

  protected def updateDocumentToCollection[T](key: String, value: BSONValue, updateValue: T, collection: BSONCollection)(implicit typeTag: TypeTag[T], swriter: BSONDocumentWriter[T], sreader: BSONDocumentReader[T]): Future[WriteResult] = {

    val selector = BSONDocument(key -> value)
    val future: Future[UpdateWriteResult] = collection.update(selector, updateValue)

    future.map( fm => {
      fm.ok match {
        case false => println(s"Update fail for field ${key} with value ${value}: ${fm.errmsg.getOrElse("<none>")}")
        case true => println(s"Update success for field ${key} with value ${value}")
      }
      fm
    })
  }


  /*  
  def getDocumentFieldByKey[T](key : String, value : String, collectionName : String, fieldName : String)(implicit swriter: BSONDocumentWriter[T], sreader: BSONDocumentReader[T]) = {
      log.info(s"Locating document(s) by key $key with value $value on collection $collectionName. Extracting property $fieldName")
      val collection = db.collection(collectionName, strategy)
      val query = BSONDocument(key -> value)
      
      val future = collection.find(query).one[T].map { value =>  value.map( v => BSONValueStringReader.read(v.get(fieldName).get) ) }
      
      future
  }
  */
}

object MongoDBHelper {
  private val log = WaspLogger(this.getClass)

  def getMongoDriver(actorSystem: ActorSystem): MongoDriver = {
    log.info("Creating MongoDB connection")
    new MongoDriver(None) //TODO rimettere actor system
  }

  def getConnection(mongoDBConfig: MongoDBConfigModel, mongoDriver: MongoDriver): MongoConnection = {
    //return a connection pool
    mongoDriver.connection(List(mongoDBConfig.address))
  }
}