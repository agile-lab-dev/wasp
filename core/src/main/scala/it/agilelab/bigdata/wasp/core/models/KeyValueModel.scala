package it.agilelab.bigdata.wasp.core.models

import org.mongodb.scala.bson.BsonObjectId

// TODO external scaladocs links
/**
	*
	*/
case class KeyValueModel(override val name: String,
												 schema: String,
												 dataFrameSchema: String,
												 avroSchemas: Option[Map[String, String]],
                    _id: Option[BsonObjectId] = None) extends Model
