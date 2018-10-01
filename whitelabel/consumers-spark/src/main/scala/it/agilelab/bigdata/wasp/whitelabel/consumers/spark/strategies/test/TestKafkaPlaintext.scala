package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
	* A simple strategy that adds columns for key, value, headers and destination topic for testing.
	*
	* @author Nicolò Bidotti
	*/
class TestKafkaPlaintext extends Strategy {
	/**
		* Adds columns for key, value, headers and destination topic for testing to the first input DataFrame and returns
		* the result.
		*/
	override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {
		val df = dataFrames.head._2
		
		df.withColumn("myKey", lit("messageKey"))
			.withColumn("myValue", lit("messageValue"))
			.withColumn("myHeaders",array(struct(lit("key").as("headerKey"), lit("value".getBytes).as("headerValue")),
		                              struct(lit("key2").as("headerKey"), lit("value2".getBytes).as("headerValue"))))
			.withColumn("topics", array(lit("test_plaintext.topic"), lit("test2_plaintext.topic")))
		  .selectExpr("explode(topics) as myTopic", "*")
		  .drop("topics")
	}
}
