package it.agilelab.bigdata.wasp.core.datastores

import it.agilelab.bigdata.wasp.core.utils.ConfigManager


/**
	* A `DatastoreCategory` represents a high-level, categorical division of the various datastores.
	*
	* @author Nicolò Bidotti
	*/
sealed trait DatastoreCategory {
	def category: String
}
trait ConsoleCategory   extends DatastoreCategory with StreamingSink                         { override val category = "console"   }
trait DatabaseCategory  extends DatastoreCategory with StreamingSink with BatchSourceAndSink { override val category = "database"  }
trait IndexCategory     extends DatastoreCategory with StreamingSink with BatchSourceAndSink { override val category = "index"     }
trait KeyValueCategory  extends DatastoreCategory with StreamingSink with BatchSourceAndSink { override val category = "keyvalue"  }
trait RawCategory       extends DatastoreCategory with StreamingSink with BatchSourceAndSink { override val category = "raw"       }
trait TopicCategory     extends DatastoreCategory with StreamingAndBatchSourceAndSink        { override val category = "topic"     }
trait WebSocketCategory extends DatastoreCategory with StreamingSourceAndSink                { override val category = "websocket" }

/**
	* A `DatastoreProduct` identifies either a particular datastore, as in an actual software product, or a generic one,
	* as in the framework will choose which one to use depending on configuration.
	*
	* @author Nicolò Bidotti
	*/
sealed trait DatastoreProduct extends DatastoreCategory {
	import DatastoreProduct._
	
	def product: Option[String]
	
	def getActualProduct: String = {
		product match {
			case Some(p) => p
			case None    => this match {
				case _: ConsoleCategory   => ConsoleProduct.product.get
				case _: DatabaseCategory  => JDBCProduct.product.get
				case _: IndexCategory     => ConfigManager.getWaspConfig.defaultIndexedDatastore
				case _: KeyValueCategory  => ConfigManager.getWaspConfig.defaultKeyvalueDatastore
				case _: RawCategory       => RawProduct.product.get
				case _: TopicCategory     => KafkaProduct.product.get
				case _: WebSocketCategory => WebSocketProduct.product.get
				case _                    => throw new IllegalArgumentException("Unknown datastore category for datastore " +
					                                                                "product \"" + this + "\" unable to provide " +
					                                                                "default datastore product")
			}
		}
	}
}

object DatastoreProduct {
	object ConsoleProduct         extends ConsoleCategory   with DatastoreProduct { override val product = Some("console")   }
	object ElasticProduct         extends IndexCategory     with DatastoreProduct { override val product = Some("elastic")   }
	object HBaseProduct           extends KeyValueCategory  with DatastoreProduct { override val product = Some("hbase")     }
	object JDBCProduct            extends DatabaseCategory  with DatastoreProduct { override val product = Some("jdbc")      }
	object KafkaProduct           extends TopicCategory     with DatastoreProduct { override val product = Some("kafka")     }
	object RawProduct             extends RawCategory       with DatastoreProduct { override val product = Some("raw")       }
	object SolrProduct            extends IndexCategory     with DatastoreProduct { override val product = Some("solr")      }
	object WebSocketProduct       extends WebSocketCategory with DatastoreProduct { override val product = Some("websocket") }
	object GenericIndexProduct    extends IndexCategory     with DatastoreProduct { override val product = None              }
	object GenericKeyValueProduct extends KeyValueCategory  with DatastoreProduct { override val product = None              }
	object GenericTopicProduct    extends TopicCategory     with DatastoreProduct { override val product = None              }
}


trait StreamingSource
trait StreamingSink
trait BatchSource
trait BatchSink

trait StreamingSourceAndSink         extends StreamingSource with StreamingSink
trait BatchSourceAndSink             extends BatchSource     with BatchSink

trait StreamingAndBatchSourceAndSink extends StreamingSourceAndSink with BatchSourceAndSink