package it.agilelab.bigdata.wasp.consumers.spark.eventengine

import java.io.{File, InputStreamReader}

import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.core.eventengine.Event
import org.scalatest.{Matchers, WordSpec}

class InnerMailStrategySpec extends WordSpec with Matchers with SparkSetup {

  private val reader =  new InputStreamReader(getClass.getResourceAsStream("inner_mail_strategy.conf"))
  private val fakeConfig = try {
    ConfigFactory.parseReader(reader)
  } finally {
    reader.close()
  }

  // A sequence of different events
  val testSeq = Seq(
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
      eventId = "1",
      timestamp = 0
    ),
    Event(
      eventRuleName = "HighTemperature",
      source = "streamingSource1",
      payload = Some("""{"name":"sensor_3","temperature":620.0,"someLong":0,"someStuff":"dispari","someNumber":5}"""),
      eventType = "TempControl",
      severity = "CRITICAL",
      sourceId = Some("sensor_3"),
      eventId = "2",
      timestamp = 0),
    Event(
      eventRuleName = "OddHighNumbers",
      source = "streamingSource2",
      payload = Some("""{"name":"sensor_0","temperature":200.0,"someLong":0,"someStuff":"dispari","someNumber":99}"""),
      eventType = "OddHighNumbers",
      severity = "HIGH_TEMP",
      sourceId = Some("sensor_0"),
      eventId = "3",
      timestamp = 0
    ),
    Event(
      eventRuleName = "OddHighNumbers",
      source = "streamingSource2",
      payload = Some("""{"name":"sensor_4","temperature":45.0,"someLong":0,"someStuff":"dispari","someNumber":101}"""),
      eventType = "OddHighNumbers",
      severity = "LOW_TEMP",
      sourceId = Some("sensor_4"),
      eventId = "4",
      timestamp = 0
    )
  )
  val controlSeq = Seq (
    Mail(
      mailTo = "tempcheck@controunit.company",
      mailCc = Some("criticalissues@controlunit.company"),
      mailBcc = None,
      mailContent = """TEMPLATE 1
                      |eventId = 0
                      |eventType = TempControl
                      |severity = CRITICAL
                      |payload = {"name":"sensor_0","temperature":200.0,"someLong":0,"someStuff":"dispari","someNumber":99}
                      |timestamp = 0
                      |source = streamingSource1
                      |sourceId = sensor_0
                      |ruleName = HighTemperature
                      |""".stripMargin,
      mailSubject = "CRITICAL TEMPERATURE REGISTERED BY: sensor_0"
    ),
    Mail(
      mailTo = "tempcheck@controunit.company",
      mailCc = Some("criticalissues@controlunit.company"),
      mailBcc = None,
      mailContent = """TEMPLATE 1
                      |eventId = 2
                      |eventType = TempControl
                      |severity = CRITICAL
                      |payload = {"name":"sensor_3","temperature":620.0,"someLong":0,"someStuff":"dispari","someNumber":5}
                      |timestamp = 0
                      |source = streamingSource1
                      |sourceId = sensor_3
                      |ruleName = HighTemperature
                      |""".stripMargin,
      mailSubject = "CRITICAL TEMPERATURE REGISTERED BY: sensor_3"
    ),
    Mail(
      mailTo = "test@controlunit.company",
      mailCc = None,
      mailBcc = Some("secret@controlunit.company"),
      mailContent = """TEMPLATE 1
                      |eventId = 4
                      |eventType = OddHighNumbers
                      |severity = LOW_TEMP
                      |payload = {"name":"sensor_4","temperature":45.0,"someLong":0,"someStuff":"dispari","someNumber":101}
                      |timestamp = 0
                      |source = streamingSource2
                      |sourceId = sensor_4
                      |ruleName = OddHighNumbers""".stripMargin,
      mailSubject = "Streaming Source: streamingSource2, Source ID: sensor_4"
    )
  )

  private val highTempMailQty = 2
  private val compositeStatementMailQty = 1
  private val totalMailQty = highTempMailQty + compositeStatementMailQty

  // An empty Seq
  private val emptySeq: Seq[Event] = Seq.empty

  // A Seq which triggers 0 mails
  private val fruitlessSeq = Seq(
    Event(
      eventRuleName = "HighTemperature",
      source = "streamingSource1",
      payload = Some("""{"name":"sensor_0","temperature":200.0,"someLong":0,"someStuff":"dispari","someNumber":99}"""),
      eventType = "TempControl",
      severity = "NOT CRITICAL",
      sourceId = Some("sensor_0"),
      eventId = "0",
      timestamp = 0),
    Event(
      eventRuleName = "HighTemperature",
      source = "streamingSource1",
      payload = Some("""{"name":"sensor_2","temperature":120.0,"someLong":0,"someStuff":"dispari","someNumber":50}"""),
      eventType = "TempControl",
      severity = "NOT WARN",
      sourceId = Some("sensor_2"),
      eventId = "1",
      timestamp = 0
    ),
    Event(
      eventRuleName = "HighTemperature",
      source = "streamingSource1",
      payload = Some("""{"name":"sensor_3","temperature":620.0,"someLong":0,"someStuff":"dispari","someNumber":5}"""),
      eventType = "TempControl",
      severity = "NOT CRITICAL",
      sourceId = Some("sensor_3"),
      eventId = "2",
      timestamp = 0),
    Event(
      eventRuleName = "OddHighNumbers",
      source = "streamingSource2",
      payload = Some("""{"name":"sensor_0","temperature":200.0,"someLong":0,"someStuff":"dispari","someNumber":99}"""),
      eventType = "OddHighNumbers",
      severity = "HIGH_TEMP",
      sourceId = Some("sensor_0"),
      eventId = "3",
      timestamp = 0
    ),
    Event(
      eventRuleName = "OddHighNumbers",
      source = "streamingSource2",
      payload = Some("""{"name":"sensor_4","temperature":45.0,"someLong":0,"someStuff":"dispari","someNumber":101}"""),
      eventType = "OddHighNumbers",
      severity = "NOT LOW_TEMP",
      sourceId = Some("sensor_4"),
      eventId = "4",
      timestamp = 0
    )
  )

  "When dealing with multiple mail rules" should {

    val target: InnerMailStrategy = new InnerMailStrategy(fakeConfig.getConfig("multipleRules"))

    s"retrieve exactly $totalMailQty in testSeq which match the control Seq" in withSparkSession { ss => {

      import ss.implicits._

      val eventsDf = ss.sparkContext.parallelize(testSeq).toDF
      val mails = target.transform(eventsDf).as[Mail].collect()
      mails.length.equals(totalMailQty) should be (true)

      // Check that all the control events are included in the test seq
      controlSeq
        .map(control =>
          mails.map(test => fakeEquals(control, test))      // Check if control event is equal to test event (can be false for a single check), cannot be for every check
            .fold(false)((b1, b2) => b1 || b2))             // Return true if the control event was found among the many test event
        .forall(identity) should be (true)                  // Return true if every control event has been found among test events
    }}

    "Find no mails in fruitlessSeq" in withSparkSession { ss => {
      import ss.implicits._
      val mails: Array[Mail] = target.transform(ss.sparkContext.parallelize(fruitlessSeq).toDF).as[Mail].collect()
      mails.length should be (0)
    }}

    "Find no mails in emptySeq" in withSparkSession { ss => {
      import ss.implicits._
      val mails: Array[Mail] = target.transform(ss.sparkContext.parallelize(emptySeq).toDF).as[Mail].collect()
      mails.length should be (0)
    }}
  }

  "When dealing with single mail rule" should {

    val target: InnerMailStrategy = new InnerMailStrategy(fakeConfig.getConfig("singleRule"))

    s"retrieve exactly $highTempMailQty in testSeq" in withSparkSession { ss => {

      import ss.implicits._

      val eventsDf = ss.sparkContext.parallelize(testSeq).toDF
      val mails = target.transform(eventsDf).as[Mail].collect()
      mails.length.equals(highTempMailQty) should be (true)

    }}

    "Find no mails in fruitlessSeq" in withSparkSession { ss => {
      import ss.implicits._
      val mails: Array[Mail] = target.transform(ss.sparkContext.parallelize(fruitlessSeq).toDF).as[Mail].collect()
      mails.length should be (0)
    }}

    "Find no mails in emptySeq" in withSparkSession { ss => {
      import ss.implicits._
      val mails: Array[Mail] = target.transform(ss.sparkContext.parallelize(emptySeq).toDF).as[Mail].collect()
      mails.length should be (0)
    }}
  }

  // Fake equals method which test the mail equality unless the event id
  private def fakeEquals(m1: Mail, m2: Mail): Boolean = {
    val s1 = m1.mailContent.split("\n").filterNot(s => s.contains("eventId"))
    val s2 = m2.mailContent.split("\n").filterNot(s => s.contains("eventId"))

    //println("S1: " + s1.mkString(", "))
    //println("S2: " + s2.mkString(", "))

    m1.mailTo.equals(m2.mailTo) &&
    m1.mailCc.equals(m2.mailCc) &&
    m1.mailBcc.equals(m2.mailBcc) &&
    m1.mailSubject.equals(m2.mailSubject) &&
    s1.sameElements(s2)
  }

}

//TODO
case class Mail (
                  mailTo: String,
                  mailCc: Option[String],
                  mailBcc: Option[String],
                  mailSubject: String,
                  mailContent: String,
                  contentType: String = "text/html"
                )