package it.agilelab.bigdata.wasp.consumers.spark.eventengine

import java.time.Clock
import java.util.UUID

import com.typesafe.config.Config
import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.core.eventengine.eventproducers.EventRule
import it.agilelab.bigdata.wasp.core.eventengine.settings._
import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import it.agilelab.bigdata.wasp.core.eventengine.EventEngineConstants._


class EventStrategy extends Strategy {

  // Configuration is injected from the Pipegraph. WARN: this has to be lazy because configuration will be populated
  // only at the first transform call
  lazy val clock: Clock = Clock.systemUTC()
  lazy val idGen: IDGenerator = UUIDGenerator
  lazy val innerEventStrategy = InnerEventStrategy(configuration, clock, idGen)

  /**
    * @param dataFrames
    * @return
    */
  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {

    //Supposing I support only 1 single streaming source. At least for now
    val df: DataFrame = dataFrames.values.head
    innerEventStrategy.transform(df)
  }

}


case class InnerEventStrategy(configuration: Config, clock: Clock, idGen: IDGenerator) {

  private val settings: EventStrategySettings = EventStrategySettingsFactory.create(configuration)

  private def randomStr(len: Int): String = RandomStringUtils.randomAlphanumeric(len) //Special char free

  private val generateId: () => String = () => idGen.generate()
  private val generateId_UFD = udf(generateId)

  private val currentTimeMillis: () => Long = () =>  clock.millis()
  private val currentTimeMillisUDF = udf(currentTimeMillis)


  /**
    * Retrieve events from input data + a seq of rules
    * @param df is the input DataFrame with operational data
    * @return a DataFrame of Events
    */
  def transform(df: DataFrame): DataFrame = {

    val rules = settings.rules // TODO: here should be placed additional filters

    val rawEvents = fetchRawEvents(df, rules)               // Merge N rules and applies them together to obtain a raw events df. 1 row = 1-N events
    val explodedDf = explodeByRule(rawEvents, rules)        // Cross-join raw events with rules and filter out meaningless rows. 1 row = 1 events
    val refinedEvents = enrichAndRefine(explodedDf, rules)  // Transform the DataModel from the one of the operational data to the one of the events.
    refinedEvents
  }


  /**
    * Create an SQL statement from an input DataFrame of M columns and N event rules. The resulting df contains M + N columns,
    * where each additional column contains a flag which indicates if that row triggered the corresponding event.
    * Each row of the output DF is a row of the input DF which triggered at least one event, the same line can therefore
    * correspond to more than one event
    *
    * Example
    * An input DF is processed with a seq of 2 event rules
    *
    * Input DataFrame with operational data:
    * +---------+-----------+-------------+---------+----------+
    * |     data|temperature|     someLong|someStuff|someNumber|
    * +---------+-----------+-------------+---------+----------+
    * | sensor_0|   111.6358|1553246909414|     even|        79|
    * | sensor_1|  40.293335|1553246909414|     even|        10|
    * | sensor_2|  99.990001|1553246909414|      odd|        25|
    * | sensor_3|  109.99341|1553246909414|      odd|        41|
    * | sensor_4|  145.99556|1553246909414|      odd|        95|
    * +--------+-----------+-------------+---------+-----------+
    *
    *
    * Rules statements:
    * HighTemperature :   (temperature > 100)
    * OddHighNumbers :    (someNumber > 75 AND someStuff == "odd")
    *
    *
    * Output DataFrame with
    * +---------+-----------+-------------+---------+----------+-----------------------+----------------------+
    * |     data|temperature|     someLong|someStuff|someNumber|HighTemperature_matches|OddHighNumbers_matches|
    * +---------+-----------+-------------+---------+----------+-----------------------+----------------------+
    * | sensor_0|   111.6358|1553246909414|     even|        79|                   true|                 false|
    * | sensor_3|  109.99341|1553246909414|      odd|        41|                   true|                 false|
    * | sensor_4|  145.99556|1553246909414|      odd|        95|                   true|                  true|    << This line accounts for 2 events
    * +---------+-----------+-------------+---------+----------+-----------------------+----------------------+
    *
    * @param dataDf is the input DataFrame containing operational data
    * @param rules  is the sequence of event rule
    * @return a DataFrame containing the rows which triggered at least one event
    */
  private def fetchRawEvents(dataDf: DataFrame, rules: Seq[EventRule]): DataFrame = {
    val ss = dataDf.sparkSession

    val tableName: String = s"TEMP_RAW_TABLE_${randomStr(RANDOM_STRING_LENGTH)}"

    dataDf.registerTempTable(tableName)

    val sqlQuery: String = {
      val sb = new StringBuilder()
      sb.append("SELECT ")

      val requests: Seq[String] = {
        val originalFields: Seq[String] = dataDf.schema.fields.map(f => f.name)

        val ruleFields: Seq[String] = rules.map(rule =>
          s" (${rule.ruleStatement}) AS ${rule.eventRuleName}_$MATCHES_FLAG")

        originalFields ++ ruleFields
      }

      sb.append(requests.mkString(", "))

      sb.append(s" FROM $tableName WHERE ")

      sb.append(rules.map(r => s"(${r.ruleStatement})").mkString(" OR "))

      sb.toString()
    }

    val rawEvents: DataFrame = ss.sqlContext.sql(sqlQuery)
    ss.catalog.dropTempView(tableName)

    rawEvents
  }


  /**
    * Create an SQL statement which cross-join the raw events and the rules, creating a line for each event
    *
    * Example
    *
    * The input DF is the result of [[fetchRawEvents]] example, and the rules are the same previously applied
    *
    * Input DF:
    * +---------+-----------+-------------+---------+----------+-----------------------+----------------------+
    * |     data|temperature|     someLong|someStuff|someNumber|HighTemperature_matches|OddHighNumbers_matches|
    * +---------+-----------+-------------+---------+----------+-----------------------+----------------------+
    * | sensor_0|   111.6358|1553246909414|     even|        79|                   true|                 false|
    * | sensor_3|  109.99341|1553246909414|      odd|        41|                   true|                 false|
    * | sensor_4|  155.99556|1553246909414|      odd|        95|                   true|                  true|
    * +---------+-----------+-------------+---------+----------+-----------------------+----------------------+
    *
    *
    * Rule DF is created from rule seq:
    * +---------------+-------------------+--------------------+----------------+--------------------+----------------+
    * |       ruleName|ruleStreamingSource|       ruleStatement|    ruleTypeExpr|    ruleSeverityExpr|ruleSourceIdExpr|
    * +---------------+-------------------+--------------------+----------------+--------------------+----------------+
    * |HighTemperature|   streamingSource1|   temperature > 100|   'TempControl'|IF( temperature <...|            name|
    * | OddHighNumbers|   streamingSource2|someNumber > 75 A...|'OddHighNumbers'|IF( temperature <...|            name|
    * +---------------+-------------------+--------------------+----------------+--------------------+----------------+
    *
    *
    * Join result:
    * +---------+-----------+-------------+---------+----------+---------------+-------------------+
    * |     data|temperature|     someLong|someStuff|someNumber|       ruleName|ruleStreamingSource|
    * +---------+-----------+-------------+---------+----------+---------------+-------------------+
    * | sensor_0|   111.6358|1553246909414|     even|        79|HighTemperature|   streamingSource1|
    * | sensor_3|  109.99341|1553246909414|      odd|        41|HighTemperature|   streamingSource1|
    * | sensor_4|  155.99556|1553246909414|      odd|        95|HighTemperature|   streamingSource1|
    * | sensor_4|  155.99556|1553246909414|      odd|        95| OddHighNumbers|   streamingSource2|
    * +---------+-----------+-------------+---------+----------+---------------+-------------------+
    *
    *
    * @param rawEventsDf is the input DataFrame containing raw lines
    * @param rules is the sequence of event rule
    * @return
    */
  private def explodeByRule(rawEventsDf: DataFrame, rules: Seq[EventRule]): DataFrame = {

    val ss = rawEventsDf.sparkSession
    import ss.implicits._

    val rulesDF = ss.sparkContext.parallelize(rules).toDF
    val eventsTableName: String = s"TEMP_EXPLODE_TABLE_${randomStr(RANDOM_STRING_LENGTH)}" //TODO: this is a mockup
    val rulesTableName: String = s"TEMP_RULES_TABLE_${randomStr(RANDOM_STRING_LENGTH)}" //TODO: this is a mockup

    rawEventsDf.registerTempTable(eventsTableName)
    rulesDF.registerTempTable(rulesTableName)

    val sqlQuery = {
      val sb = new StringBuilder()
      sb.append(s"SELECT /*+ BROADCAST ($rulesTableName) */ ")

      val matches = rules.map(r => s"${r.eventRuleName}_$MATCHES_FLAG").toSet
      rawEventsDf.schema.filterNot(f => matches(f.name)).foreach(f => sb.append(s"${f.name}, "))
      //sb.append(s"$EVENT_RULE_NAME, $STREAMING_SOURCE, $TIMESTAMP ")
      sb.append(s"$EVENT_RULE_NAME, $STREAMING_SOURCE ") // TIMESTAMP IS NOT YET AVAILABLE!!!

      sb.append(s"FROM $eventsTableName cross join $rulesTableName ")
      sb.append("ON ")

      rules.init.foreach(r => sb.append(s"(${r.eventRuleName}_$MATCHES_FLAG and $EVENT_RULE_NAME = '${r.eventRuleName}') OR "))
      sb.append(s"(${rules.last.eventRuleName}_$MATCHES_FLAG and $EVENT_RULE_NAME = '${rules.last.eventRuleName}') ")

      sb.toString
    }

    val explodedEvents = ss.sql(sqlQuery)


    ss.catalog.dropTempView(eventsTableName)
    ss.catalog.dropTempView(rulesTableName)

    explodedEvents
  }


  /**
    * Transform the data model from the one of the operational data to the one of the events.
    *
    * Example
    *
    * The input DF is the result of [[explodeByRule]] example, and the rules are the same previously applied
    *
    * Input DF:
    *
    * +---------+-----------+-------------+---------+----------+---------------+-------------------+
    * |     data|temperature|     someLong|someStuff|someNumber|       ruleName|ruleStreamingSource|
    * +---------+-----------+-------------+---------+----------+---------------+-------------------+
    * | sensor_0|   111.6358|1553246909414|     even|        79|HighTemperature|   streamingSource1|
    * | sensor_3|  109.99341|1553246909414|      odd|        41|HighTemperature|   streamingSource1|
    * | sensor_4|  155.99556|1553246909414|      odd|        95|HighTemperature|   streamingSource1|
    * | sensor_4|  155.99556|1553246909414|      odd|        95| OddHighNumbers|   streamingSource2|
    * +---------+-----------+-------------+---------+----------+---------------+-------------------+
    *
    * Events Data Model:
    *
    * +-----------+---------------+----------------+-------------+--------------+--------+---------+---------+
    * |    eventId|       ruleName|          source|    timestamp|     eventType|severity| sourceId|  payload|
    * +-----------+---------------+----------------+-------------+--------------+--------+---------+---------+
    *
    * IMPORTANT: any column of the original data model will be saved as part of the payload column (in json format)
    * Hence, if a column of the operational data model has the same name of a column of the event data model, there
    * won't be overlapping as the original column will be inserted in the payload and dropped from the DataFrame BEFORE
    * the new column is created
    *
    * EventType, Severity and SourceID are computed from SQL statements inside event rule:
    *
    * EventType:
    * HighTemperature :   ('TempControl')
    * OddHighNumbers :    ('OddHighNumbers')
    *
    * EventSeverity:
    * HighTemperature :   (IF( temperature < 150, "WARN", "CRITICAL" ))
    * OddHighNumbers :    (IF( temperature < 150, "LOW_TEMP", "HIGH_TEMP" ))
    *
    * SourceId:
    * HighTemperature :   (name)
    * OddHighNumbers :    (name)
    *
    *
    * Result of the query:
    *
    * +------------+---------------+----------------+--------------------+---------------+----------+---------+--------------------+
    * |     eventId|       ruleName|          source|           timestamp|      eventType|  severity| sourceId|             payload|
    * +------------+---------------+----------------+--------------------+---------------+----------+---------+--------------------+
    * |  8589934592|HighTemperature|streamingSource1|2019-03-25 10:22:...|    TempControl|      WARN| sensor_0|{"name":"sensor_0...|
    * |  8589934593|HighTemperature|streamingSource1|2019-03-25 10:22:...|    TempControl|      WARN| sensor_3|{"name":"sensor_3...|
    * |  8589934594|HighTemperature|streamingSource1|2019-03-25 10:22:...|    TempControl|  CRITICAL| sensor_4|{"name":"sensor_4...|
    * | 25769803777| OddHighNumbers|streamingSource2|2019-03-25 10:22:...| OddHighNumbers| HIGH_TEMP| sensor_4|{"name":"sensor_4...|
    * +------------+---------------+----------------+--------------------+---------------+----------+---------+--------------------+
    *
    * Note that timestamp column has been replaced with Event creation timestamp. Original timestamp is still available in payload
    *
    * @param explodedEventsDf is the input DataFrame containing one line for each events
    * @param rules are the seq of event rules
    * @return a DataFrame of Events
    */
  private def enrichAndRefine(explodedEventsDf: DataFrame, rules: Seq[EventRule]): DataFrame = {

    val ss = explodedEventsDf.sparkSession
    val tableName: String = s"TEMP_ENRICH_TABLE_${randomStr(RANDOM_STRING_LENGTH)}"

    // All the operational data model columns are turned into a single column "payload", which present them as a json
    val payloadColumns = explodedEventsDf.columns.filterNot(c => c.equals(EVENT_RULE_NAME) || c.equals(STREAMING_SOURCE))
    val eventsWithPayload = explodedEventsDf.withColumn(PAYLOAD, to_json(struct(payloadColumns.head, payloadColumns.tail: _*)))

    eventsWithPayload.registerTempTable(tableName)

    val sqlQuery = {
      val sb = new StringBuilder()
      sb.append("SELECT ")

      sb.append(s"$EVENT_RULE_NAME, $STREAMING_SOURCE AS $SOURCE, $PAYLOAD, ")

      sb.append("CASE ")
      rules.foreach(r => sb.append(s"WHEN $EVENT_RULE_NAME = '${r.eventRuleName}' THEN ${r.ruleTypeExpr} "))
      sb.append("END ")
      sb.append(s" AS $EVENT_TYPE, ")

      sb.append("CASE ")
      rules.foreach(r => sb.append(s"WHEN $EVENT_RULE_NAME = '${r.eventRuleName}' THEN ${r.ruleSeverityExpr} "))
      sb.append("END ")
      sb.append(s" AS $SEVERITY, ")

      sb.append("CASE ")
      rules.foreach(r => sb.append(s"WHEN $EVENT_RULE_NAME = '${r.eventRuleName}' THEN ${r.ruleSourceIdExpr} "))
      sb.append("END ")
      sb.append(s" AS $SOURCE_ID ")

      sb.append(s"FROM $tableName ")

      sb.toString()
    }

    val enrichedEvents = ss.sql(sqlQuery)
    ss.catalog.dropTempView(tableName)


    enrichedEvents
      //.withColumn(EVENT_ID, monotonically_increasing_id()) //Unsafe in streaming ETL, likely to generate duplicates
      .withColumn(EVENT_ID, generateId_UFD())
      //.withColumn(TIMESTAMP, current_timestamp())  //This implementation keeps a TimestampType on the column which is illegal with Avro4s
      .withColumn(TIMESTAMP, currentTimeMillisUDF())
  }

}

trait IDGenerator {
  def generate(): String
}

case object UUIDGenerator extends IDGenerator{
  override def generate(): String = UUID.randomUUID().toString
}