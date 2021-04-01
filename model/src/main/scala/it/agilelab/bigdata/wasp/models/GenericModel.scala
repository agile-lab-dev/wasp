package it.agilelab.bigdata.wasp.models

import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.genericProduct
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import org.mongodb.scala.bson.BsonDocument

case class GenericModel(override val name: String,
												kind: String,
												value: BsonDocument,
												options: GenericOptions = GenericOptions.default
											 )
	extends DatastoreModel {
	override def datastoreProduct: DatastoreProduct = genericProduct

}

case class GenericOptions(options: Option[Map[String, String]] = None)

object GenericOptions {
	lazy val default = GenericOptions()
}
