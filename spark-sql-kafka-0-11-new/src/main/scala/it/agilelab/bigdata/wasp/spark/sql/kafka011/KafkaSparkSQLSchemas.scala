package it.agilelab.bigdata.wasp.spark.sql.kafka011

import org.apache.spark.sql.types._

// TODO: make this private[wasp] once we move the rest of the library under our namespace
/**
	* Spark SQL schemas for Kafka.
	*
	* @author Nicolò Bidotti
	*/
object KafkaSparkSQLSchemas {
	// attribute names
	val KEY_ATTRIBUTE_NAME: String = "key"
	val VALUE_ATTRIBUTE_NAME: String = "value"
	val HEADERS_ATTRIBUTE_NAME: String = "headers"
	val HEADER_KEY_ATTRIBUTE_NAME: String = "headerKey"
	val HEADER_VALUE_ATTRIBUTE_NAME: String = "headerValue"
	val TOPIC_ATTRIBUTE_NAME: String = "topic"
	val PARTITION_ATTRIBUTE_NAME: String = "partition"
	val OFFSET_ATTRIBUTE_NAME: String = "offset"
	val TIMESTAMP_ATTRIBUTE_NAME: String = "timestamp"
	val TIMESTAMP_TYPE_ATTRIBUTE_NAME: String = "timestampType"
	
	// header data type, following spec defined in KIP-82
	val HEADER_DATA_TYPE = ArrayType(
		StructType(
			Seq(StructField(HEADER_KEY_ATTRIBUTE_NAME, StringType, nullable = false),
		      StructField(HEADER_VALUE_ATTRIBUTE_NAME, BinaryType, nullable = true))),
		containsNull = false)
	
	// complete kafka input data type
	val INPUT_SCHEMA = StructType(Seq(
		StructField(KEY_ATTRIBUTE_NAME, BinaryType),
		StructField(VALUE_ATTRIBUTE_NAME, BinaryType),
		StructField(HEADERS_ATTRIBUTE_NAME, HEADER_DATA_TYPE),
		StructField(TOPIC_ATTRIBUTE_NAME, StringType),
		StructField(PARTITION_ATTRIBUTE_NAME, IntegerType),
		StructField(OFFSET_ATTRIBUTE_NAME, LongType),
		StructField(TIMESTAMP_ATTRIBUTE_NAME, TimestampType),
		StructField(TIMESTAMP_TYPE_ATTRIBUTE_NAME, IntegerType)
	))
}