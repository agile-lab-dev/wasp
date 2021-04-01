package it.agilelab.bigdata.wasp.models

import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.WebSocketProduct
import it.agilelab.bigdata.wasp.datastores.{DatastoreProduct}
import org.mongodb.scala.bson.BsonDocument


case class WebsocketModel (override val name: String,
                           host: String,
                           port: String,
                           resourceName: String,
                           options: Option[BsonDocument] = None)
	  extends DatastoreModel {
	override def datastoreProduct: DatastoreProduct = WebSocketProduct
}