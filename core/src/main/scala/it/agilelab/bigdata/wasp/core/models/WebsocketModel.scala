package it.agilelab.bigdata.wasp.core.models

import org.mongodb.scala.bson.{BsonDocument, BsonObjectId}

case class WebsocketModel (override val name: String,
                           host: String,
                           port: String,
                           resourceName: String,
                           //var isActive: Boolean,
                           options: Option[BsonDocument] = None,
                           _id: Option[BsonObjectId] = None) extends Model