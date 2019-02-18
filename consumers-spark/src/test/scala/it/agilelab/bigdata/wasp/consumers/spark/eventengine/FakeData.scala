package it.agilelab.bigdata.wasp.consumers.spark.eventengine

import java.util.UUID

import scala.util.Random

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