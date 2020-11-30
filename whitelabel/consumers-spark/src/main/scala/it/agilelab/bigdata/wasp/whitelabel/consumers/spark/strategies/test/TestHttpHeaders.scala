package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, map}

/**
	* A simple strategy that adds static Http haders for testing the functionality.
	*
	*/
class TestHttpHeaders extends Strategy {
	/**
		* Adds static headers to the first input DataFrame and returns the result.
		*/
	override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {
		val df = dataFrames.head._2

		df.withColumn("headers", map(lit("key"), lit("value")))
	}
}
