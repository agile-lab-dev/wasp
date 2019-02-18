package it.agilelab.bigdata.wasp.core.datastores

import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, ReflectionUtils}


/**
	* A `DatastoreCategory` represents a high-level, categorical division of the various datastores.
	*
	* @author Nicolò Bidotti
	*/
sealed trait DatastoreCategory {
	def categoryName: String
}

trait ConsoleCategory   extends DatastoreCategory with StreamingSink                         { override val categoryName = "console"   }
trait DatabaseCategory  extends DatastoreCategory with StreamingSink with BatchSourceAndSink { override val categoryName = "database"  }
trait IndexCategory     extends DatastoreCategory with StreamingSink with BatchSourceAndSink { override val categoryName = "index"     }
trait KeyValueCategory  extends DatastoreCategory with StreamingSink with BatchSourceAndSink { override val categoryName = "keyvalue"  }
trait RawCategory       extends DatastoreCategory with StreamingSink with BatchSourceAndSink { override val categoryName = "raw"       }
trait TopicCategory     extends DatastoreCategory with StreamingAndBatchSourceAndSink        { override val categoryName = "topic"     }
trait WebSocketCategory extends DatastoreCategory with StreamingSourceAndSink                { override val categoryName = "websocket" }
trait WebMailCategory 	extends DatastoreCategory with StreamingSink												 { override val categoryName = "webmail"   }
trait DocumentCategory  extends DatastoreCategory with StreamingSink with BatchSourceAndSink { override val categoryName = "document"  }

/**
	* A `DatastoreProduct` identifies either a particular datastore, as in an actual software product, or a generic one,
	* as in the framework will choose which one to use depending on configuration.
	*
	* @author Nicolò Bidotti
	*/
sealed trait DatastoreProduct extends DatastoreCategory {
	import DatastoreProduct._
	
	def productName: Option[String]
	
	/**
		* Returns the product name, looking up the default datastore product for the category if this is a *GenericProduct.
		*/
	def getActualProductName: String = {
		productName match {
			case Some(p) => p
			case None    => getDefaultProductForThisCategory.productName.get
		}
	}
	
	/**
		* Returns the default product for this category, looking it up in the configuration.
		*/
	def getDefaultProductForThisCategory: DatastoreProduct = {
		val productsInThisCategory = productsLookupMap
			.filterKeys(_._1 == categoryName)
		
		val defaultProductNameFromConfig = this match {
			case _: ConsoleCategory   => ConsoleProduct.productName.get
			case _: DatabaseCategory  => JDBCProduct.productName.get
			case _: IndexCategory     => ConfigManager.getWaspConfig.defaultIndexedDatastore
			case _: KeyValueCategory  => ConfigManager.getWaspConfig.defaultKeyvalueDatastore
			case _: RawCategory       => RawProduct.productName.get
			case _: TopicCategory     => KafkaProduct.productName.get
			case _: WebSocketCategory => WebSocketProduct.productName.get
			case _: WebMailCategory   => WebMailProduct.productName.get
			case _: DocumentCategory  => MongoDbProduct.productName.get
			case _                    =>
				throw new IllegalArgumentException(s"""Unknown datastore category "$categoryName" of $this, unable to provide
					                                    | default datastore product""".stripMargin.filterNot(_.isControl))
		}
		
		val maybeDefaultProduct = productsInThisCategory.get((categoryName, defaultProductNameFromConfig))
		
		if (maybeDefaultProduct.isEmpty) {
			throw new IllegalArgumentException(s"""Unknown default datastore product name "$defaultProductNameFromConfig" for
				                                    | category "$categoryName" of $this, unable to provide default datastore
				                                    | product""".stripMargin.filterNot(_.isControl))
		} else {
			maybeDefaultProduct.get
		}
	}
}

object DatastoreProduct {
	object ConsoleProduct         extends ConsoleCategory   with DatastoreProduct { override val productName = Some("console")   }
	object ElasticProduct         extends IndexCategory     with DatastoreProduct { override val productName = Some("elastic")   }
	object HBaseProduct           extends KeyValueCategory  with DatastoreProduct { override val productName = Some("hbase")     }
	object JDBCProduct            extends DatabaseCategory  with DatastoreProduct { override val productName = Some("jdbc")      }
	object KafkaProduct           extends TopicCategory     with DatastoreProduct { override val productName = Some("kafka")     }
	object RawProduct             extends RawCategory       with DatastoreProduct { override val productName = Some("raw")       }
	object SolrProduct            extends IndexCategory     with DatastoreProduct { override val productName = Some("solr")      }
	object WebSocketProduct       extends WebSocketCategory with DatastoreProduct { override val productName = Some("websocket") }
	object MongoDbProduct         extends DocumentCategory  with DatastoreProduct { override val productName = Some("mongodb")   }
	object GenericIndexProduct    extends IndexCategory     with DatastoreProduct { override val productName = None              }
	object GenericKeyValueProduct extends KeyValueCategory  with DatastoreProduct { override val productName = None              }
	object GenericTopicProduct    extends TopicCategory     with DatastoreProduct { override val productName = None              }
	object WebMailProduct					extends WebMailCategory   with DatastoreProduct { override val productName = Some("webmail")   }

	// product lookup map for default datastore resolution
	private val productsLookupMap = buildProductsLookupMap()
	
	// build products lookup map of the form (("category", "product"), DatastoreProduct)
	private def buildProductsLookupMap(): Map[(String, String), DatastoreProduct] = {
		// grab all objects that are subtypes of DatastoreProduct
		val objectSubclassesList = ReflectionUtils.findObjectSubclassesOfSealedTraitAssumingTheyAreAllObjects[DatastoreProduct]
		
		// build list of (identifier, object)
		val subclassesIdentifiersList = objectSubclassesList
			.filter(_.productName.nonEmpty) // filter out *GenericProduct
			.map(datastoreProduct => ((datastoreProduct.categoryName, datastoreProduct.productName.get), datastoreProduct))
		
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