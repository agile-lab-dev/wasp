package it.agilelab.bigdata.wasp.core.models

/**
	* Supported datastores.
	*
	* @author Nicolò Bidotti
	*/
object Datastores {
	// available categories
	val indexCategory = "index"
	val keyValueCategory = "keyvalue"
	val rawCategory = "raw"
	val topicCategory = "topic"
	val websocketCategory = "websocket"
	val consoleCategory = "console"
	val jdbcCategory = "jdbc"
	
	// available products
	val elasticProduct = "elastic"
	val solrProduct = "solr"
	val hbaseProduct = "hbase"
	val rawProduct = "raw"
	val kafkaProduct = "kafka"
	val websocketProduct = "websocket" // TODO actual product is camel, change to "camel-websocket"?
	val consoleProduct = "console"
	val jdbcProduct = "jdbc"
}