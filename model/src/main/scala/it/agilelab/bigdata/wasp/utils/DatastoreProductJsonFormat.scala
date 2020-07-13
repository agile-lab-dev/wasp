package it.agilelab.bigdata.wasp.utils

import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import spray.json.{JsObject, JsString, JsValue, RootJsonFormat}

/**
	* @author Nicolò Bidotti
	*/
object DatastoreProductJsonFormat extends DatastoreProductSerde with RootJsonFormat[DatastoreProduct] {
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
