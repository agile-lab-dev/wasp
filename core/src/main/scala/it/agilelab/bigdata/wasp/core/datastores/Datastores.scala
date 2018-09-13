package it.agilelab.bigdata.wasp.core.datastores

import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, ReflectionUtils}


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
	
	/**
		* Returns the product name, looking up the default datastore product for the category if this is a *GenericProduct.
		*/
	def getActualProduct: String = {
		product match {
			case Some(p) => p
			case None    => getDefaultProductForThisCategory.product.get
		}
	}
	
	/**
		* Returns the default product for this category, looking it up in the configuration.
		*/
	def getDefaultProductForThisCategory: DatastoreProduct = {
		val productsInThisCategory = productsLookupMap
			.filterKeys(_._1 == category)
		
		val defaultProductNameFromConfig = this match {
			case _: ConsoleCategory   => ConsoleProduct.product.get
			case _: DatabaseCategory  => JDBCProduct.product.get
			case _: IndexCategory     => ConfigManager.getWaspConfig.defaultIndexedDatastore
			case _: KeyValueCategory  => ConfigManager.getWaspConfig.defaultKeyvalueDatastore
			case _: RawCategory       => RawProduct.product.get
			case _: TopicCategory     => KafkaProduct.product.get
			case _: WebSocketCategory => WebSocketProduct.product.get
			case _                    =>
				throw new IllegalArgumentException(s"""Unknown datastore category "$category" of $this, unable to provide
					                                    | default datastore product""".stripMargin.filterNot(_.isControl))
		}
		
		val maybeDefaultProduct = productsInThisCategory.get((category, defaultProductNameFromConfig))
		
		if (maybeDefaultProduct.isEmpty) {
			throw new IllegalArgumentException(s"""Unknown default datastore product name "$defaultProductNameFromConfig" for
				                                    | category "$category" of $this, unable to provide default datastore
				                                    | product""".stripMargin.filterNot(_.isControl))
		} else {
			maybeDefaultProduct.get
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
	
	// product lookup map for default datastore resolution
	private val productsLookupMap = buildProductsLookupMap()
	
	// build products lookup map of the form (("category", "product"), DatastoreProduct)
	private def buildProductsLookupMap(): Map[(String, String), DatastoreProduct] = {
		// grab all objects that are subtypes of DatastoreProduct
		val objectSubclassesList = ReflectionUtils.findObjectSubclassesOfSealedTraitAssumingTheyAreAllObjects[DatastoreProduct]
		
		// build list of (identifier, object)
		val subclassesIdentifiersList = objectSubclassesList
			.filter(_.product.nonEmpty) // filter out *GenericProduct
			.map(datastoreProduct => ((datastoreProduct.category, datastoreProduct.product.get), datastoreProduct))
		
		// check that we don't have collisions so we can use the list for forward and reverse lookup maps
		val listSize = subclassesIdentifiersList.size
		require(listSize == subclassesIdentifiersList.map(_._1).toSet.size, "There cannot be collisions between identifiers!")
		require(listSize == subclassesIdentifiersList.map(_._2).toSet.size, "There cannot be collisions between subclasses!")
		
		subclassesIdentifiersList.toMap
	}
}

trait StreamingSource
trait StreamingSink
trait BatchSource
trait BatchSink

trait StreamingSourceAndSink         extends StreamingSource with StreamingSink
trait BatchSourceAndSink             extends BatchSource     with BatchSink

trait StreamingAndBatchSourceAndSink extends StreamingSourceAndSink with BatchSourceAndSink