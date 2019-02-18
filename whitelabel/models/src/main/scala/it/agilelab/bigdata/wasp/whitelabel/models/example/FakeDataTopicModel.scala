package it.agilelab.bigdata.wasp.whitelabel.models.example

import java.util.UUID

import com.sksamuel.avro4s.AvroSchema
import it.agilelab.bigdata.wasp.core.models.TopicModel
import it.agilelab.bigdata.wasp.core.utils.JsonConverter

import scala.util.Random

object FakeDataTopicModel {

  val fakeDataTopicModelName = "fake-data"
  lazy val fakeDataSchema = AvroSchema[FakeData].toString


  lazy val fakeDataTopicModel = TopicModel (
    name = TopicModel.name(fakeDataTopicModelName),
    creationTime = System.currentTimeMillis,
    partitions = 3,
    replicas = 1,
    topicDataType = "avro",
    keyFieldName = None,
    headersFieldName = None,
    valueFieldsNames = None,
    useAvroSchemaManager = true,
    schema = JsonConverter.fromString(fakeDataSchema).getOrElse(org.mongodb.scala.bson.BsonDocument())
  )

}

case class FakeData(name: String, temperature: Float, someLong: Long, someStuff: String, someNumber: Int)
object FakeData{
  private val random = new Random()
  def fromRandom(): FakeData = FakeData(UUID.randomUUID().toString, random.nextInt(200), System.currentTimeMillis(), if(random.nextInt(2)%2==0) "even" else "odd", random.nextInt(101) )
}

// TODO: create unit test for composed classes
case class FakeDataContainer(fake0: FakeData, fake1: FakeData, fake2: FakeData)
object FakeDataContainer{
  def fromRandom(): FakeDataContainer = FakeDataContainer(FakeData.fromRandom(), FakeData.fromRandom(), FakeData.fromRandom())
}