package it.agilelab.bigdata.wasp.datastores



/**
	* A `DatastoreProduct` identifies either a particular datastore, as in an actual software product, or a generic one,
	* as in the framework will choose which one to use depending on configuration.
	*
	* @author Nicolò Bidotti
	*/
sealed trait DatastoreProduct {
	def categoryName: String
	def productName: Option[String]

	/**
		* Returns the product name, looking up the default datastore product for the category if this is a *GenericProduct.
		*/
	def getActualProductName: String = {
		productName.getOrElse(throw new IllegalArgumentException("- should never happen") )
	}

	/**
		* Returns the default product for this category, looking it up in the configuration.
		*/
	def getDefaultProductForThisCategory: DatastoreProduct = this


}

case class GenericProduct(categoryName: String, productName: Option[String]) extends DatastoreProduct

object DatastoreProduct {

	lazy val ConsoleProduct: GenericProduct            = GenericProduct("console", Some("console"))

	lazy val genericProduct: GenericProduct            = GenericProduct("generic", Some("generic"))

	lazy val ElasticProduct: GenericProduct            = GenericProduct("index", Some("elastic"))

	lazy val HBaseProduct: GenericProduct              = GenericProduct("keyvalue", Some("hbase"))

	lazy val JDBCProduct: GenericProduct               = GenericProduct("database", Some("jdbc"))

	lazy val KafkaProduct: GenericProduct              = GenericProduct("topic", Some("kafka"))

  lazy val RawProduct: GenericProduct                = GenericProduct("raw", Some("raw"))

	lazy val SolrProduct: GenericProduct               = GenericProduct("index", Some("solr"))

	lazy val WebSocketProduct: GenericProduct          = GenericProduct("websocket", Some("websocket"))

	lazy val MongoDbProduct: GenericProduct            = GenericProduct("document", Some("mongodb"))

	lazy val GenericIndexProduct: GenericProduct       = GenericProduct("index", Some("solr"))

	lazy val GenericKeyValueProduct: GenericProduct    = GenericProduct("keyvalue", Some("hbase"))

	lazy val GenericTopicProduct: GenericProduct       = GenericProduct("topic", Some("kafka"))

	lazy val HttpProduct: GenericProduct               = GenericProduct("http", Some("http"))

	lazy val WebMailProduct: GenericProduct            = GenericProduct("webmail", Some("webmail"))

	lazy val IndexProduct: GenericProduct 			 			  = GenericProduct("index", Some("solr"))

	lazy val KeyValueProduct: GenericProduct 			 	  = GenericProduct("keyvalue", Some("hbase"))

	lazy val CdcProduct: GenericProduct            = GenericProduct("cdc", Some("cdc"))

}

trait StreamingSource
trait StreamingSink
trait BatchSource
trait BatchSink

trait StreamingSourceAndSink         extends StreamingSource with StreamingSink
trait BatchSourceAndSink             extends BatchSource     with BatchSink

trait StreamingAndBatchSourceAndSink extends StreamingSourceAndSink with BatchSourceAndSink