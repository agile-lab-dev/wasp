package it.agilelab.bigdata.wasp.models

import it.agilelab.bigdata.wasp.datastores.{DatastoreProduct, GenericProduct}
import org.mongodb.scala.bson.BsonDocument

case class GenericModel(override val name: String,
												value: BsonDocument,
												product: GenericProduct,
												options: GenericOptions = GenericOptions.default
											 )
	extends DatastoreModel {
	override def datastoreProduct: DatastoreProduct = product

}

case class GenericOptions(options: Option[Map[String, String]] = None)

object GenericOptions {
	lazy val default = GenericOptions()
}
