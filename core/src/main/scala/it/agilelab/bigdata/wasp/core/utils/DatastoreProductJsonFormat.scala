package it.agilelab.bigdata.wasp.core.utils

import it.agilelab.bigdata.wasp.core.datastores.DatastoreProduct
import spray.json.{JsObject, JsString, JsValue, RootJsonFormat}

/**
	* @author Nicolò Bidotti
	*/
object DatastoreProductJsonFormat extends RootJsonFormat[DatastoreProduct] {
	// field for the identifier
	private val categoryField = "category"
	private val productField = "product"
	
	// grabs all subtypes of DatastoreProduct and builds a list of (DatastoreProcut, ("category", "product")) from which
	// we can then build the lookup maps
	private def buildSubclassIdentifiersList(): List[(DatastoreProduct, (String, String))] = {
		val objectSubclassesList = ReflectionUtils.findObjectSubclassesOfSealedTraitAssumingTheyAreAllObjects[DatastoreProduct]
		
		// build list of (object, identifier)
		val subclassesIdentifiersList = objectSubclassesList.map(datastoreProduct => (datastoreProduct, (datastoreProduct.category, datastoreProduct.product.getOrElse(""))))
		
		// check that we don't have collisions so we can use the list for forward and reverse lookup maps
		val listSize = subclassesIdentifiersList.size
		require(listSize == subclassesIdentifiersList.map(_._1).toSet.size, "There cannot be collisions between subclasses!")
		require(listSize == subclassesIdentifiersList.map(_._2).toSet.size, "There cannot be collisions between identifiers!")
		
		subclassesIdentifiersList
	}
	
	// build forward and reverse lookup maps from subclass identifiers list
	private val encodingLookupMap = buildSubclassIdentifiersList().toMap
	private val decodingLookupMap = encodingLookupMap.map(_.swap)
	
	override def read(json: JsValue): DatastoreProduct = {
		val fields = json.asJsObject.fields
		val (categoryValue, productValue) = (fields(categoryField), fields(productField))
		val JsString(category) = categoryValue
		val JsString(product) = productValue
		val key = (category, product)
		decodingLookupMap(key)
	}
	
	override def write(obj: DatastoreProduct): JsValue = {
		val (category, product) = encodingLookupMap(obj)
		val fields = Map(categoryField -> JsString(category), productField -> JsString(product))
		JsObject(fields)
	}
}
