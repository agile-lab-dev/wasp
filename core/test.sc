import org.mongodb.scala.bson.ObjectId
import java.util.concurrent.TimeUnit

import com.mongodb.ConnectionString

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import org.mongodb.scala._
import org.mongodb.scala.connection.{ClusterSettings, SocketSettings}

import scala.reflect.ClassTag

implicit class DocumentObservable[C](val observable: Observable[Document]) extends ImplicitObservable[Document] {
  override val converter: (Document) => String = (doc) => doc.toJson
}

implicit class GenericObservable[C](val observable: Observable[C]) extends ImplicitObservable[C] {
  override val converter: (C) => String = (doc) => doc.toString
}

trait ImplicitObservable[C] {
  val observable: Observable[C]
  val converter: (C) => String

  def results(): Seq[C] = Await.result(observable.toFuture(), Duration(10, TimeUnit.SECONDS))
  def headResult() = Await.result(observable.head(), Duration(10, TimeUnit.SECONDS))
  def printResults(initial: String = ""): Unit = {
    if (initial.length > 0) print(initial)
    results().foreach(res => println(converter(res)))
  }
  def printHeadResult(initial: String = ""): Unit = println(s"${initial}${converter(headResult())}")
}

object Person {
  def apply(firstName: String, lastName: String): Person = Person(new ObjectId(), firstName, lastName);
}
object Desk {
  def apply(position: String, owner: Person): Desk = Desk(new ObjectId(), position, owner);
}
val clusterSettings: ClusterSettings = ClusterSettings.builder()
  .applyConnectionString(new ConnectionString("mongodb://localhost")).build()

val settings =
  MongoClientSettings.builder().clusterSettings(clusterSettings).heartbeatSocketSettings(
    SocketSettings.builder().
      connectTimeout(1, TimeUnit.SECONDS)
        .keepAlive(true)
        .readTimeout(1, TimeUnit.SECONDS)
      .build())
    .build()

val mongoClient = MongoClient(settings)

case class Person(_id: ObjectId, firstName: String, lastName: String)

case class Desk(_id: ObjectId, position: String, owner: Person)


import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.bson.codecs.configuration.CodecRegistries.{ fromRegistries, fromProviders }
val codecRegistry = fromRegistries(fromProviders(classOf[Person], classOf[Desk]), DEFAULT_CODEC_REGISTRY)

val database: MongoDatabase = mongoClient.getDatabase("mydb").withCodecRegistry(codecRegistry)
try {
  database.listCollectionNames().results()
}catch {
  case e: Exception =>
    println(e)
    e.printStackTrace()
}

val collection: MongoCollection[Person] = database.getCollection("test")

val collectionDesks: MongoCollection[Desk] = database.getCollection("desks")

val person: Person = Person("Ada", "Lovelace")
val desk: Desk = Desk("desk 1", person)

collection.insertOne(person).results()
collectionDesks.insertOne(desk).results()

collection.find().first().printHeadResult()
collectionDesks.find().first().printHeadResult()

def getAllDocuments[C](collection: MongoCollection[C])(implicit ct: ClassTag[C]): Seq[C] = {
  collection.find[C]().results()
}

getAllDocuments(collectionDesks)