package it.agilelab.bigdata.wasp.models

import org.mongodb.scala.bson.BsonDocument

case class ProcessGroupModel(name: String, content: BsonDocument, errorPort: String) extends Model