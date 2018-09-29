package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, lit, struct}

/**
	* A simple strategy that adds static Kafka haders for testing the functionality.
	*
	* @author Nicolò Bidotti
	*/
class TestKafkaHeaders extends Strategy {
	/**
		* Adds static headers to the first input DataFrame and returns the result.
		*/
	override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {
		val df = dataFrames.head._2
		
		df.withColumn("headers",array(struct(lit("key").as("headerKey"), lit("value".getBytes).as("headerValue")),
		                              struct(lit("key2").as("headerKey"), lit("value2".getBytes).as("headerValue"))))
	}
}
