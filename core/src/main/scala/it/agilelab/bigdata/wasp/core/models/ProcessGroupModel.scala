package it.agilelab.bigdata.wasp.core.models

import org.mongodb.scala.bson.BsonDocument

case class ProcessGroupModel(name:String, content: BsonDocument) extends Model