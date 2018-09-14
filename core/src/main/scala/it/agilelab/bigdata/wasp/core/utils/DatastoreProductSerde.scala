package it.agilelab.bigdata.wasp.core.utils

import it.agilelab.bigdata.wasp.core.datastores.DatastoreProduct

/**
	* Base trait for `DataStoreProduct` serde.
	*
	* @author Nicolò Bidotti
	*/
trait DatastoreProductSerde {
	// field names
	protected val categoryField = "category"
	protected val productField = "product"
	
	// grabs all subtypes of DatastoreProduct and builds a list of (DatastoreProduct, ("category", "product")) from which
	// we can then build the lookup maps
	protected def buildSubclassIdentifiersList(): List[(DatastoreProduct, (String, String))] = {
		val objectSubclassesList = ReflectionUtils.findObjectSubclassesOfSealedTraitAssumingTheyAreAllObjects[DatastoreProduct]
		
		// build list of (object, identifier)
		val subclassesIdentifiersList = objectSubclassesList.map(datastoreProduct => (datastoreProduct, (datastoreProduct.categoryName, datastoreProduct.productName.getOrElse(""))))
		
		// check that we don't have collisions so we can use the list for forward and reverse lookup maps
		val listSize = subclassesIdentifiersList.size
		require(listSize == subclassesIdentifiersList.map(_._1).toSet.size, "There cannot be collisions between subclasses!")
		require(listSize == subclassesIdentifiersList.map(_._2).toSet.size, "There cannot be collisions between identifiers!")
		
		subclassesIdentifiersList
	}
	
	// build forward and reverse lookup maps from subclass identifiers list
	protected val encodingLookupMap = buildSubclassIdentifiersList().toMap
	protected val decodingLookupMap = encodingLookupMap.map(_.swap)
}
