package it.agilelab.bigdata.wasp.consumers.spark.plugins.mailer

import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkStructuredStreamingWriter
import org.apache.spark.sql.{DataFrame, Encoders, ForeachWriter, Row}
import org.apache.spark.sql.streaming.DataStreamWriter
import it.agilelab.bigdata.wasp.core.eventengine.EventEngineConstants._
import org.apache.spark.sql.types.DataType

class MailWriter(options: Map[String, String]) extends SparkStructuredStreamingWriter {

  val mailAgent = MailAgentImpl(options)
  val innerMailWriter = new InnerMailWriter(mailAgent)

  override def write(stream: DataFrame): DataStreamWriter[Row] = {
    innerMailWriter.write(stream)
  }
}

// This is easily unit-testable
class InnerMailWriter(mailAgent: MailAgent) {

  def write(stream: DataFrame): DataStreamWriter[Row] = {

    /**
      * Check the input schema is compliant with the Mail schema.
      * WARN: Check only fields name and type, since the nullable flag is inferred at runtime and may not be the same every time
      */
    val expectedSchema: Array[(String, DataType)] = Encoders.product[Mail].schema.fields.map(f => f.name -> f.dataType)
    val actualSchema: Array[(String, DataType)] = stream.schema.fields.map(f => f.name -> f.dataType)
    if(!expectedSchema.forall(actualSchema.contains)) throw new UnsupportedOperationException(s"Received schema: ${stream.schema} doesn't match expected schema $expectedSchema")


    stream.writeStream.foreach(new MailForeachWriter(mailAgent))
  }

}

class MailForeachWriter(mailAgent: MailAgent)
  extends ForeachWriter[Row] {

  private implicit def rowToMail(row: Row): Mail = Mail(
      //row.getAs[String]("mailFrom"),
      mailTo = row.getAs[String](MAIL_TO),
      mailCc = Option(row.getAs[String](MAIL_CC)),
      mailBcc = Option(row.getAs[String](MAIL_BCC)),
      mailSubject = row.getAs[String](MAIL_SUBJECT),
      mailContent = row.getAs[String](MAIL_CONTENT)
  )

  override def open(partitionId: Long, version: Long): Boolean = true


  override def process(value: Row): Unit = {
    mailAgent.send(value)
  }

  override def close(errorOrNull: Throwable): Unit = Unit
}
