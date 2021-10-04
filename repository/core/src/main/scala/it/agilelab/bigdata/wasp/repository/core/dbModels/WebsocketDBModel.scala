package it.agilelab.bigdata.wasp.repository.core.dbModels

import it.agilelab.bigdata.wasp.models.Model
import org.mongodb.scala.bson.BsonDocument

trait WebsocketDBModel extends Model

case class WebsocketDBModelV1(override val name: String,
                              host: String,
                              port: String,
                              resourceName: String,
                              options: Option[BsonDocument] = None
                             ) extends WebsocketDBModel
