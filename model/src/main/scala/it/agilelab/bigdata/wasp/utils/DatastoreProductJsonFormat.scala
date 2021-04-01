package it.agilelab.bigdata.wasp.utils

import it.agilelab.bigdata.wasp.datastores.{DatastoreProduct, GenericProduct}
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

		product match {
			case "unknown" => GenericProduct(category, None)
			case _ => GenericProduct(category, Option(product))
		}
	}

	override def write(obj: DatastoreProduct): JsValue = {
		val fields = Map(categoryField -> JsString(obj.categoryName), productField -> JsString(obj.productName.getOrElse("unknown")))
		JsObject(fields)
	}
}
