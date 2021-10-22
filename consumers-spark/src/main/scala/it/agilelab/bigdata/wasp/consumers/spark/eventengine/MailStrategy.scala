package it.agilelab.bigdata.wasp.consumers.spark.eventengine

import java.io.{StringReader, StringWriter}
import java.util.Properties
import com.typesafe.config.Config
import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.core.build.BuildInfo
import it.agilelab.bigdata.wasp.core.eventengine.eventconsumers.MailingRule
import it.agilelab.bigdata.wasp.core.eventengine.settings.MailingStrategySettingsFactory
import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.velocity.{Template, VelocityContext}
import org.apache.velocity.app.VelocityEngine
import org.apache.velocity.runtime.RuntimeInstance
import it.agilelab.bigdata.wasp.core.eventengine.EventEngineConstants._
import org.apache.velocity.runtime.log.{NullLogChute, NullLogSystem, SimpleLog4JLogSystem}

import scala.io.Source

// TODO: this strategy have to depend on the mail plugin
//import it.agilelab.bigdata.wasp.consumers.spark.plugins.mailer.Mail

class MailStrategy extends Strategy {

  // Has to be lazy because it will be evaluated only when transform is called
  lazy val innerMailStrategy = new InnerMailStrategy(configuration)

  /**
    *
    * @param dataFrames
    * @return
    */
  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {
    val df = dataFrames.values.head
    innerMailStrategy.transform(df)
  }
}

class InnerMailStrategy(config: Config) {

  private val settings = MailingStrategySettingsFactory.create(config)
  private val templateStrings: Map[String, String] =
    settings.rules.map(r => r.templatePath -> Source.fromFile(r.templatePath).getLines.mkString("\n")).toMap

  def transform(dataFrame: DataFrame): DataFrame = {

    val broadcastTemplateMap: Broadcast[Map[String, String]] =
      dataFrame.sparkSession.sparkContext.broadcast(templateStrings)

    val rawMails     = fetchRawMails(dataFrame, settings.rules)                            // Merge N rules and applies them together to obtain a raw mail df. 1 row = 1-N events
    val explodedMail = explodeByRule(rawMails, settings.rules)                             // Cross-join raw mails with rules and filter out meaningless rows. 1 row = 1 mail
    val refinedMails = enrichAndRefine(explodedMail, settings.rules, broadcastTemplateMap) // Transform the DataModel from the one of the events to the one of the mails.

    settings.enableMailAggregation match {
      case true  => aggregateMails(refinedMails)
      case false => refinedMails
    }
  }

  /**
    * Create an SQL statement from an input DataFrame of Event objects and N mailing rules. The resulting df contains M + N columns,
    * where M is the number of columns of Event case class, and each additional column contains a flag which indicates
    * if that row has to be turned into an e-mail.
    * Each row of the output DF is a row of the input DF which triggered at least one mail statement, the same line can therefore
    * correspond to more than one e-mail to send
    *
    * Example
    *
    * Input DF of events:
    *
    * +---------------+----------------+--------------------+--------------+---------+---------+-----------+--------------------+
    * |  eventRuleName|          source|             payload|     eventType| severity| sourceId|    eventId|           timestamp|
    * +---------------+----------------+--------------------+--------------+---------+---------+-----------+--------------------+
    * |HighTemperature|streamingSource1|{"name":"sensor_2...|   TempControl|     WARN| sensor_2| 8589934592|2019-04-01 16:55:...|
    * |HighTemperature|streamingSource1|{"name":"sensor_5...|   TempControl| CRITICAL| sensor_5| 8589934593|2019-04-01 16:55:...|
    * |HighTemperature|streamingSource1|{"name":"sensor_8...|   TempControl| CRITICAL| sensor_8| 8589934594|2019-04-01 16:55:...|
    * |HighTemperature|streamingSource1|{"name":"sensor_1...|   TempControl|     WARN|sensor_12| 8589934595|2019-04-01 16:55:...|
    * |HighTemperature|streamingSource1|{"name":"sensor_1...|   TempControl| CRITICAL|sensor_13| 8589934596|2019-04-01 16:55:...|
    * |HighTemperature|streamingSource1|{"name":"sensor_1...|   TempControl|     WARN|sensor_16| 8589934597|2019-04-01 16:55:...|
    * |HighTemperature|streamingSource1|{"name":"sensor_1...|   TempControl|     WARN|sensor_17| 8589934598|2019-04-01 16:55:...|
    * |HighTemperature|streamingSource1|{"name":"sensor_1...|   TempControl|     WARN|sensor_19| 8589934599|2019-04-01 16:55:...|
    * | OddHighNumbers|streamingSource2|{"name":"sensor_1...|OddHighNumbers| LOW_TEMP| sensor_1|25769803776|2019-04-01 16:55:...|
    * | OddHighNumbers|streamingSource2|{"name":"sensor_5...|OddHighNumbers|HIGH_TEMP| sensor_5|25769803777|2019-04-01 16:55:...|
    * | OddHighNumbers|streamingSource2|{"name":"sensor_1...|OddHighNumbers| LOW_TEMP|sensor_11|25769803778|2019-04-01 16:55:...|
    * | OddHighNumbers|streamingSource2|{"name":"sensor_1...|OddHighNumbers| LOW_TEMP|sensor_17|25769803779|2019-04-01 16:55:...|
    * | OddHighNumbers|streamingSource2|{"name":"sensor_1...|OddHighNumbers| LOW_TEMP|sensor_19|25769803780|2019-04-01 16:55:...|
    * +---------------+----------------+--------------------+--------------+---------+---------+-----------+--------------------+
    *
    * Mailing rules:
    *
    * mailingRule1: (severity = 'CRITICAL')
    * mailingRule2: (severity = 'LOW_TEMP' AND eventType = 'OddHighNumbers')
    *
    * Output DF:
    *
    * +---------------+----------------+--------------------+--------------+--------+---------+-----------+--------------------+-------------+-------------+
    * |  eventRuleName|          source|             payload|     eventType|severity| sourceId|    eventId|           timestamp|rule1_matches|rule2_matches|
    * +---------------+----------------+--------------------+--------------+--------+---------+-----------+--------------------+-------------+-------------+
    * |HighTemperature|streamingSource1|{"name":"sensor_5...|   TempControl|CRITICAL| sensor_5| 8589934593|2019-04-01 16:55:...|         true|        false|
    * |HighTemperature|streamingSource1|{"name":"sensor_8...|   TempControl|CRITICAL| sensor_8| 8589934594|2019-04-01 16:55:...|         true|        false|
    * |HighTemperature|streamingSource1|{"name":"sensor_1...|   TempControl|CRITICAL|sensor_13| 8589934596|2019-04-01 16:55:...|         true|        false|
    * | OddHighNumbers|streamingSource2|{"name":"sensor_1...|OddHighNumbers|LOW_TEMP| sensor_1|25769803776|2019-04-01 16:55:...|        false|         true|
    * | OddHighNumbers|streamingSource2|{"name":"sensor_1...|OddHighNumbers|LOW_TEMP|sensor_11|25769803778|2019-04-01 16:55:...|        false|         true|
    * | OddHighNumbers|streamingSource2|{"name":"sensor_1...|OddHighNumbers|LOW_TEMP|sensor_17|25769803779|2019-04-01 16:55:...|        false|         true|
    * | OddHighNumbers|streamingSource2|{"name":"sensor_1...|OddHighNumbers|LOW_TEMP|sensor_19|25769803780|2019-04-01 16:55:...|        false|         true|
    * +---------------+----------------+--------------------+--------------+--------+---------+-----------+--------------------+-------------+-------------+
    *
    * @param eventDf is the input DataFrame containing events
    * @param rules   is the sequence of mailing rule
    * @return a DataFrame containing the rows which triggered at least one e-mail to be sent
    */
  private def fetchRawMails(eventDf: DataFrame, rules: Seq[MailingRule]): DataFrame = {
    val ss = eventDf.sparkSession

    val tableName: String = s"TEMP_RAW_TABLE_${randomStr(RANDOM_STRING_LENGTH)}"

    eventDf.registerTempTable(tableName)

    val sqlQuery: String = {
      val sb = new StringBuilder()
      sb.append("SELECT ")

      val requests: Seq[String] = {
        val originalFields: Seq[String] = eventDf.schema.fields.map(f => f.name)

        val ruleFields: Seq[String] =
          rules.map(rule => s" (${rule.ruleStatement}) AS ${rule.mailingRuleName}_$MATCHES_FLAG")

        originalFields ++ ruleFields
      }

      sb.append(requests.mkString(", "))

      sb.append(s" FROM $tableName WHERE ")

      sb.append(rules.map(r => s"(${r.ruleStatement})").mkString(" OR "))

      sb.toString()
    }

    val rawMails: DataFrame = ss.sqlContext.sql(sqlQuery)
    ss.catalog.dropTempView(tableName)

    rawMails
  }

  private def randomStr(len: Int): String = RandomStringUtils.randomAlphanumeric(len)

  /**
    * Create an SQL statement which cross-join the raw mails and the mail rules, creating a line for each mail
    *
    * Example
    *
    * The input DF is the result of [[fetchRawMails]] example, and the rules are the same previously applied
    *
    * Input DF:
    * +---------------+----------------+--------------------+--------------+--------+---------+-----------+--------------------+-------------+-------------+
    * |  eventRuleName|          source|             payload|     eventType|severity| sourceId|    eventId|           timestamp|rule1_matches|rule2_matches|
    * +---------------+----------------+--------------------+--------------+--------+---------+-----------+--------------------+-------------+-------------+
    * |HighTemperature|streamingSource1|{"name":"sensor_5...|   TempControl|CRITICAL| sensor_5| 8589934593|2019-04-01 16:55:...|         true|        false|
    * |HighTemperature|streamingSource1|{"name":"sensor_8...|   TempControl|CRITICAL| sensor_8| 8589934594|2019-04-01 16:55:...|         true|        false|
    * |HighTemperature|streamingSource1|{"name":"sensor_1...|   TempControl|CRITICAL|sensor_13| 8589934596|2019-04-01 16:55:...|         true|        false|
    * | OddHighNumbers|streamingSource2|{"name":"sensor_1...|OddHighNumbers|LOW_TEMP| sensor_1|25769803776|2019-04-01 16:55:...|        false|         true|
    * | OddHighNumbers|streamingSource2|{"name":"sensor_1...|OddHighNumbers|LOW_TEMP|sensor_11|25769803778|2019-04-01 16:55:...|        false|         true|
    * | OddHighNumbers|streamingSource2|{"name":"sensor_1...|OddHighNumbers|LOW_TEMP|sensor_17|25769803779|2019-04-01 16:55:...|        false|         true|
    * | OddHighNumbers|streamingSource2|{"name":"sensor_1...|OddHighNumbers|LOW_TEMP|sensor_19|25769803780|2019-04-01 16:55:...|        false|         true|
    * +---------------+----------------+--------------------+--------------+--------+---------+-----------+--------------------+-------------+-------------+
    *
    * Output DF:
    *
    * +---------------+----------------+--------------------+--------------+--------+---------+-----------+--------------------+---------------+-----------------+-----------------+------------------+--------------------+
    * |       ruleName|          source|             payload|     eventType|severity| sourceId|    eventId|           timestamp|mailingRuleName|           mailTo|           mailCc|           mailBcc|        templatePath|
    * +---------------+----------------+--------------------+--------------+--------+---------+-----------+--------------------+---------------+-----------------+-----------------+------------------+--------------------+
    * |HighTemperature|streamingSource1|{"name":"sensor_5...|   TempControl|CRITICAL| sensor_5| 8589934593|2019-04-01 16:55:...|          rule1|rule1@mailTo.test|rule1@mailCc.test|rule1@mailBcc.test|./src/resources/t...|
    * |HighTemperature|streamingSource1|{"name":"sensor_8...|   TempControl|CRITICAL| sensor_8| 8589934594|2019-04-01 16:55:...|          rule1|rule1@mailTo.test|rule1@mailCc.test|rule1@mailBcc.test|./src/resources/t...|
    * |HighTemperature|streamingSource1|{"name":"sensor_1...|   TempControl|CRITICAL|sensor_13| 8589934596|2019-04-01 16:55:...|          rule1|rule1@mailTo.test|rule1@mailCc.test|rule1@mailBcc.test|./src/resources/t...|
    * | OddHighNumbers|streamingSource2|{"name":"sensor_1...|OddHighNumbers|LOW_TEMP| sensor_1|25769803776|2019-04-01 16:55:...|          rule2|rule2@mailTo.test|             null|              null|./src/resources/t...|
    * | OddHighNumbers|streamingSource2|{"name":"sensor_1...|OddHighNumbers|LOW_TEMP|sensor_11|25769803778|2019-04-01 16:55:...|          rule2|rule2@mailTo.test|             null|              null|./src/resources/t...|
    * | OddHighNumbers|streamingSource2|{"name":"sensor_1...|OddHighNumbers|LOW_TEMP|sensor_17|25769803779|2019-04-01 16:55:...|          rule2|rule2@mailTo.test|             null|              null|./src/resources/t...|
    * | OddHighNumbers|streamingSource2|{"name":"sensor_1...|OddHighNumbers|LOW_TEMP|sensor_19|25769803780|2019-04-01 16:55:...|          rule2|rule2@mailTo.test|             null|              null|./src/resources/t...|
    * +---------------+----------------+--------------------+--------------+--------+---------+-----------+--------------------+---------------+-----------------+-----------------+------------------+--------------------+
    *
    * @param rawMailsDf is the input DataFrame containing raw lines
    * @param rules      is the sequence of mail rule
    * @return
    */
  private def explodeByRule(rawMailsDf: DataFrame, rules: Seq[MailingRule]): DataFrame = {

    val ss = rawMailsDf.sparkSession
    import ss.implicits._

    val rulesDF                = ss.sparkContext.parallelize(rules).toDF
    val mailTableName: String  = s"TEMP_EXPLODE_TABLE_${randomStr(RANDOM_STRING_LENGTH)}"
    val rulesTableName: String = s"TEMP_RULES_TABLE_${randomStr(RANDOM_STRING_LENGTH)}"

    rawMailsDf.registerTempTable(mailTableName)
    rulesDF.registerTempTable(rulesTableName)

    val sqlQuery = {
      val sb = new StringBuilder()
      sb.append("SELECT ")

      val matches = rules.map(r => s"${r.mailingRuleName}_$MATCHES_FLAG").toSet

      sb.append(rawMailsDf.schema.filterNot(f => matches(f.name)).map(_.name).mkString(" ", ", ", ", "))

      sb.append(s"$MAILING_RULE_NAME, $MAIL_TO, $MAIL_CC, $MAIL_BCC, $TEMPLATE_KEY ")

      //sb.append(s"$MAILING_RULE_NAME ")

      sb.append(s"FROM $mailTableName cross join $rulesTableName ")
      sb.append("ON ")

      rules.init.foreach(r =>
        sb.append(s"(${r.mailingRuleName}_$MATCHES_FLAG and $MAILING_RULE_NAME = '${r.mailingRuleName}') OR ")
      )
      sb.append(
        s"(${rules.last.mailingRuleName}_$MATCHES_FLAG and $MAILING_RULE_NAME = '${rules.last.mailingRuleName}') "
      )

      sb.toString
    }

    println("EXPLODE QUERY: " + sqlQuery)
    val explodedMails = ss.sql(sqlQuery)

    ss.catalog.dropTempView(mailTableName)
    ss.catalog.dropTempView(rulesTableName)

    explodedMails
  }

  /**
    *
    * Transform the data model from the one of the Event to the one of the Mail.
    *
    * Mail Subject is created applying the subject-expr, and the Mail Content is generated by an UDF which encapsulates the Apache Velocity behaviour
    * In order to avoid to re-fetch from disk the template file for each entry, template strings are read once in driver string and broadcast to executors
    *
    * Example
    *
    * The input DF is the result of [[explodeByRule]] example, and the rules are the same previously applied
    *
    * Input DF:
    *
    * +---------------+----------------+--------------------+--------------+--------+---------+-----------+--------------------+---------------+-----------------+-----------------+------------------+--------------------+
    * |       ruleName|          source|             payload|     eventType|severity| sourceId|    eventId|           timestamp|mailingRuleName|           mailTo|           mailCc|           mailBcc|        templatePath|
    * +---------------+----------------+--------------------+--------------+--------+---------+-----------+--------------------+---------------+-----------------+-----------------+------------------+--------------------+
    * |HighTemperature|streamingSource1|{"name":"sensor_5...|   TempControl|CRITICAL| sensor_5| 8589934593|2019-04-01 16:55:...|          rule1|rule1@mailTo.test|rule1@mailCc.test|rule1@mailBcc.test|./src/resources/t...|
    * |HighTemperature|streamingSource1|{"name":"sensor_8...|   TempControl|CRITICAL| sensor_8| 8589934594|2019-04-01 16:55:...|          rule1|rule1@mailTo.test|rule1@mailCc.test|rule1@mailBcc.test|./src/resources/t...|
    * |HighTemperature|streamingSource1|{"name":"sensor_1...|   TempControl|CRITICAL|sensor_13| 8589934596|2019-04-01 16:55:...|          rule1|rule1@mailTo.test|rule1@mailCc.test|rule1@mailBcc.test|./src/resources/t...|
    * | OddHighNumbers|streamingSource2|{"name":"sensor_1...|OddHighNumbers|LOW_TEMP| sensor_1|25769803776|2019-04-01 16:55:...|          rule2|rule2@mailTo.test|             null|              null|./src/resources/t...|
    * | OddHighNumbers|streamingSource2|{"name":"sensor_1...|OddHighNumbers|LOW_TEMP|sensor_11|25769803778|2019-04-01 16:55:...|          rule2|rule2@mailTo.test|             null|              null|./src/resources/t...|
    * | OddHighNumbers|streamingSource2|{"name":"sensor_1...|OddHighNumbers|LOW_TEMP|sensor_17|25769803779|2019-04-01 16:55:...|          rule2|rule2@mailTo.test|             null|              null|./src/resources/t...|
    * | OddHighNumbers|streamingSource2|{"name":"sensor_1...|OddHighNumbers|LOW_TEMP|sensor_19|25769803780|2019-04-01 16:55:...|          rule2|rule2@mailTo.test|             null|              null|./src/resources/t...|
    * +---------------+----------------+--------------------+--------------+--------+---------+-----------+--------------------+---------------+-----------------+-----------------+------------------+--------------------+
    *
    * Output DF:
    *
    * +-----------------+-----------------+------------------+--------------------+-----------+--------------------+
    * |           mailTo|           mailCc|           mailBcc|         mailContent|contentType|         mailSubject|
    * +-----------------+-----------------+------------------+--------------------+-----------+--------------------+
    * |rule1@mailTo.test|rule1@mailCc.test|rule1@mailBcc.test|TEMPLATE 1 event...|  text/html|CRITICAL TEMPERAT...|
    * |rule1@mailTo.test|rule1@mailCc.test|rule1@mailBcc.test|TEMPLATE 1 event...|  text/html|CRITICAL TEMPERAT...|
    * |rule1@mailTo.test|rule1@mailCc.test|rule1@mailBcc.test|TEMPLATE 1 event...|  text/html|CRITICAL TEMPERAT...|
    * |rule2@mailTo.test|             null|              null|TEMPLATE 2 event...|  text/html|ODD HIGH TEMPERATURE|
    * |rule2@mailTo.test|             null|              null|TEMPLATE 2 event...|  text/html|ODD HIGH TEMPERATURE|
    * |rule2@mailTo.test|             null|              null|TEMPLATE 2 event...|  text/html|ODD HIGH TEMPERATURE|
    * |rule2@mailTo.test|             null|              null|TEMPLATE 2 event...|  text/html|ODD HIGH TEMPERATURE|
    * +-----------------+-----------------+------------------+--------------------+-----------+--------------------+
    *
    * @param explodedMails        is the dataframe of mails and event data to be parsed
    * @param rules                it ehe list of mailing rules
    * @param broadcastTemplateMap is the broadcast variable containing (TemplateKey -> TemplateString) entries. The path of the template from config files is used as TemplateKey
    * @return A DataFrame of Mail object ready to be sent
    */
  private def enrichAndRefine(
      explodedMails: DataFrame,
      rules: Seq[MailingRule],
      broadcastTemplateMap: Broadcast[Map[String, String]]
  ): DataFrame = {

    val mailWithContent = explodedMails
      .withColumn(CONTENT_TYPE, lit("text/html"))
      .withColumn(
        MAIL_CONTENT,
        compileMailUDF(broadcastTemplateMap)(
          col(EVENT_ID),
          col(EVENT_TYPE),
          col(SEVERITY),
          col(PAYLOAD),
          col(TIMESTAMP),
          col(SOURCE),
          col(SOURCE_ID),
          col(EVENT_RULE_NAME),
          col(TEMPLATE_KEY)
        )
      )

    val ss                = mailWithContent.sparkSession
    val tableName: String = s"TEMP_ENRICH_TABLE_${randomStr(RANDOM_STRING_LENGTH)}"

    mailWithContent.registerTempTable(tableName)

    val sqlQuery = {
      val sb = new StringBuilder()
      sb.append("SELECT ")

      sb.append(s"$MAIL_TO, $MAIL_CC, $MAIL_BCC, $MAIL_CONTENT, $CONTENT_TYPE, ")

      sb.append("CASE ")
      rules.foreach(r => sb.append(s"WHEN $MAILING_RULE_NAME = '${r.mailingRuleName}' THEN ${r.subjectStatement} "))
      sb.append("END ")
      sb.append(s" AS $MAIL_SUBJECT ")

      sb.append(s"FROM $tableName ")

      sb.toString()
    }

    val refinedMail = ss.sql(sqlQuery)
    ss.catalog.dropTempView(tableName)

    refinedMail

  }

  def compileMailUDF(broadcastTemplateMap: Broadcast[Map[String, String]]): UserDefinedFunction =
    udf(compileMail(broadcastTemplateMap))

  def compileMail(
      broadcastTemplateMap: Broadcast[Map[String, String]]
  ): (String, String, String, String, String, String, String, String, String) => String =
    (eventId, eventType, severity, payload, timestamp, source, sourceId, ruleName, templateKey) => {

      val templateString = broadcastTemplateMap.value(templateKey)
      VelocityTemplateComposer.compose(
        eventId,
        eventType,
        severity,
        payload,
        timestamp,
        source,
        sourceId,
        ruleName,
        templateString
      )
    }

  /**
    * Merge mails with same recipient concatenating their content.
    * TODO: Provide finer-grained aggregation at configuration level (based on event properties)
    * TODO: Provide configurable aggregated mail subject
    * TODO: Evaluate mail content truncation or evaluate mail content as attachment in case of multi mb mail size
    * TODO: reduce shuffling amount
    */
  private def aggregateMails(refinedMails: DataFrame): DataFrame = {
    refinedMails
      .groupBy(MAIL_TO, MAIL_CC, MAIL_BCC)
      .agg(concat_ws(LINE_SEPARATOR, collect_list(MAIL_CONTENT)).as(MAIL_CONTENT))
      .withColumn(MAIL_SUBJECT, lit("Multi-Event merged e-mail"))

  }

}

object VelocityTemplateComposer {

  //TODO: evaluate a mapPartition to initialize this only once per partition
  @transient lazy val engine: VelocityEngine = {
    val prop = {
      // TODO: instead of null logging, redirect velocity log to wasp logging
      val p = new Properties()
      p.setProperty("runtime.log.logsystem.class", classOf[NullLogChute].getCanonicalName)
      if (BuildInfo.flavor != "CDP717") {
        p.put("runtime.log.logsystem", new NullLogChute())
      }
      p
    }

    val ve = new VelocityEngine(prop)
    ve.init()
    ve
  }

  def compose(
      eventId: String,
      eventType: String,
      severity: String,
      payload: String,
      timestamp: String,
      source: String,
      sourceId: String,
      ruleName: String,
      templateString: String
  ): String = {

    val context = new VelocityContext()

    context.put("eventId", eventId)
    context.put("eventType", eventType)
    context.put("severity", severity)
    context.put("payload", payload)
    context.put("timestamp", timestamp)
    context.put("source", source)
    context.put("sourceId", sourceId)
    context.put("ruleName", ruleName)

    val message = new StringWriter()
    engine.evaluate(context, message, "velocity", templateString)
    message.toString
  }
}
