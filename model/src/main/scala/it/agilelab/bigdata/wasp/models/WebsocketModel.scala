package it.agilelab.bigdata.wasp.models

import it.agilelab.bigdata.wasp.datastores.WebSocketCategory
import org.mongodb.scala.bson.BsonDocument


case class WebsocketModel (override val name: String,
                           host: String,
                           port: String,
                           resourceName: String,
                           options: Option[BsonDocument] = None)
	  extends DatastoreModel[WebSocketCategory]