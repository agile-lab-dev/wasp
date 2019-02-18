package it.agilelab.bigdata.wasp.consumers.spark.eventengine

import java.io.InputStreamReader
import java.time.{Clock, Instant, ZoneId}

import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.core.eventengine.Event
import it.agilelab.bigdata.wasp.core.logging.Logging
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.util.Random


class InnerEventStrategySpec extends WordSpec
  with Matchers with BeforeAndAfter with SparkSetup with Logging {

  private val randomSeed = 0
  private val rand = new Random(randomSeed)
  private val fixedClock: Clock = Clock.fixed(Instant.EPOCH, ZoneId.of("UTC"))

  // This seq is made of random messages which may or may not trigger events
  private val randomSeq =
    Range(0, 1000)
      .map(i => FakeData(s"sensor_$i", rand.nextFloat() * 200, System.currentTimeMillis(), if (i % 2 == 0) "pari" else "dispari", rand.nextInt(101)))

  // This seq is made of pre-defined messages which have to trigger a specific list of events
  private val mixedSeq = Seq(
    FakeData(            // Triggers both events
      name = "sensor_0",
      temperature = 200,
      someLong=0,
      someStuff="dispari",
      someNumber = 99),
    FakeData(          //  Trigger no events
      name = "sensor_1",
      temperature = 10,
      someLong=0,
      someStuff="pari",
      someNumber = 100),
    FakeData(        //   Triggers HighTemperature event in WARN severity
      name = "sensor_2",
      temperature = 120,
      someLong=0,
      someStuff="dispari",
      someNumber = 50),
    FakeData(       //   Triggers HighTemperature event with prova2=false as payload
      name = "sensor_3",
      temperature = 620,
      someLong=0,
      someStuff="dispari",
      someNumber = 5),
    FakeData(      //   Trigger OddHighNumbers in LOW_TEMP severity
      name = "sensor_4",
      temperature = 45,
      someLong=0,
      someStuff="dispari",
      someNumber = 101)
  )
  // Control Seq of events generated from mixedSeq (not taking into account timestamp and id)
  private val mixedSeqEvents = Seq(
    Event(
      eventRuleName = "HighTemperature",
      source = "streamingSource1",
      payload = Some("""{"name":"sensor_0","temperature":200.0,"someLong":0,"someStuff":"dispari","someNumber":99}"""),
      eventType = "TempControl",
      severity = "CRITICAL",
      sourceId = Some("sensor_0"),
      eventId = "0",
      timestamp = 0),
    Event(
      eventRuleName = "HighTemperature",
      source = "streamingSource1",
      payload = Some("""{"name":"sensor_2","temperature":120.0,"someLong":0,"someStuff":"dispari","someNumber":50}"""),
      eventType = "TempControl",
      severity = "WARN",
      sourceId = Some("sensor_2"),
      eventId = "0",
      timestamp = 0
    ),
    Event(
      eventRuleName = "HighTemperature",
      source = "streamingSource1",
      payload = Some("""{"name":"sensor_3","temperature":620.0,"someLong":0,"someStuff":"dispari","someNumber":5}"""),
      eventType = "TempControl",
      severity = "CRITICAL",
      sourceId = Some("sensor_3"),
      eventId = "0",
      timestamp = 0),
    Event(
      eventRuleName = "OddHighNumbers",
      source = "streamingSource2",
      payload = Some("""{"name":"sensor_0","temperature":200.0,"someLong":0,"someStuff":"dispari","someNumber":99}"""),
      eventType = "OddHighNumbers",
      severity = "HIGH_TEMP",
      sourceId = Some("sensor_0"),
      eventId = "0",
      timestamp = 0
    ),
    Event(
      eventRuleName = "OddHighNumbers",
      source = "streamingSource2",
      payload = Some("""{"name":"sensor_4","temperature":45.0,"someLong":0,"someStuff":"dispari","someNumber":101}"""),
      eventType = "OddHighNumbers",
      severity = "LOW_TEMP",
      sourceId = Some("sensor_4"),
      eventId = "0",
      timestamp = 0
    )
  )

  private val highTempEventsQuantity = 3
  private val oddHighNumbersEventsQuantity = 2
  private val totalEventsQuantity = highTempEventsQuantity + oddHighNumbersEventsQuantity

  // Fake configuration objects to fetch event rules
  private val reader =  new InputStreamReader(getClass.getResourceAsStream("inner_event_strategy.conf"))
  private val fakeConfig = try {
    ConfigFactory.parseReader(reader)
  } finally {
    reader.close()
  }

  // This seq doesn't trigger any event
  private val fruitlessSeq = Range(0, 1000)
    .map(i => FakeData(s"sensor_$i", rand.nextFloat() * 50, System.currentTimeMillis(), if (i % 2 == 0) "pari" else "dispari", rand.nextInt(50)))

  // This seq doesn't contain any message
  private val emptySeq: Seq[FakeData] = Seq.empty


  "When dealing with multiple event rules, InnerEventStrategy" should {

    val target = new InnerEventStrategy(fakeConfig.getConfig("multipleRules"), fixedClock, FixedIdGenerator)

    //TODO: this should not be a unit test but actually a bench
    s"Process a big random sequence" in withSparkSession { ss => {
      import ss.implicits._
      val events: Array[Event] = target.transform(ss.sparkContext.parallelize(randomSeq).toDF).as[Event].collect()
      println(events.length)
    }}

    s"Find $totalEventsQuantity test events in mixedSeq which match the control event seq" in withSparkSession { ss => {
      import ss.implicits._
      val events: Array[Event] = target.transform(ss.sparkContext.parallelize(mixedSeq).toDF).as[Event].collect()

      events.length should be (totalEventsQuantity)

      val (highTempEvents, oddNumberEvents) = events.partition(e => e.eventRuleName.equals("HighTemperature"))
      highTempEvents.length should be (highTempEventsQuantity)
      oddNumberEvents.length should be (oddHighNumbersEventsQuantity)

      mixedSeqEvents should contain theSameElementsAs  events

    }}

    "Find no events in fruitlessSeq" in withSparkSession { ss => {
      import ss.implicits._
      val events: Array[Event] = target.transform(ss.sparkContext.parallelize(fruitlessSeq).toDF).as[Event].collect()
      events.length should be (0)
    }}

    "Find no events in emptySeq" in withSparkSession { ss => {
      import ss.implicits._
      val events: Array[Event] = target.transform(ss.sparkContext.parallelize(emptySeq).toDF).as[Event].collect()
      events.length should be (0)
    }}

  }

  "When dealing with a single event rule, InnerEventStrategy" should {

    val target = new InnerEventStrategy(fakeConfig.getConfig("singleRule"), fixedClock, FixedIdGenerator)

    s"Find exactly $highTempEventsQuantity in mixedSeq" in withSparkSession { ss => {
      import ss.implicits._
      val events: Array[Event] = target.transform(ss.sparkContext.parallelize(mixedSeq).toDF).as[Event].collect()

      events.length should be (highTempEventsQuantity)

      val (highTempEvents, oddNumberEvents) = events.partition(e => e.eventRuleName.equals("HighTemperature"))
      highTempEvents.length should be (highTempEventsQuantity)
      oddNumberEvents.length should be (0)
    }}

    "Find no events in noEventSeq" in withSparkSession { ss => {
      import ss.implicits._
      val events: Array[Event] = target.transform(ss.sparkContext.parallelize(fruitlessSeq).toDF).as[Event].collect()
      events.length should be (0)
    }}

    "Find no events in emptySeq" in withSparkSession { ss => {
      import ss.implicits._
      val events: Array[Event] = target.transform(ss.sparkContext.parallelize(emptySeq).toDF).as[Event].collect()
      events.length should be (0)
    }}

  }


}

case object FixedIdGenerator extends IDGenerator {
  override def generate(): String = "0"
}