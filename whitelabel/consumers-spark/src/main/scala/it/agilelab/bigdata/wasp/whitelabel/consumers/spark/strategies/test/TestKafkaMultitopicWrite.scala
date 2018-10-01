package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
	* A simple strategy that adds a topic column to write to multiple topics for testing the functionality.
	*
	* @author Nicolò Bidotti
	*/
class TestKafkaMultitopicWrite extends Strategy {
	/**
		* Adds a topic column to write to multiple topics to the first input DataFrame and returns the result.
		*/
	override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {
		val df = dataFrames.head._2
		
		val avroOrJson = configuration.getString("format")
		
		df.drop("kafkaMetadata")
			.withColumn("topics", array(lit("test3_" + avroOrJson + ".topic"), lit("test4_" + avroOrJson + ".topic")))
	  	.selectExpr("explode(topics) as topic")
	}
}
