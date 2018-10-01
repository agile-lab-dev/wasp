package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, lit, struct}

/**
	* A simple strategy that lifts the contents of the kafkaMetadata column for testing the functionality.
	*
	* @author Nicolò Bidotti
	*/
class TestKafkaMetadata extends Strategy {
	/**
		* Lifts the metadata columns from the first input DataFrame and returns the result.
		*/
	override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {
		val df = dataFrames.head._2
		
		df.selectExpr("kafkaMetadata.*", "*")
	  	.drop("kafkaMetadata")
	}
}
