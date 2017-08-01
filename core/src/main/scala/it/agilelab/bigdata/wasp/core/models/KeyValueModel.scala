package it.agilelab.bigdata.wasp.core.models

import reactivemongo.bson.BSONObjectID

// TODO external scaladocs links
/**
	*
	*/
case class KeyValueModel(override val name: String,
												 schema: String,
												 dataFrameSchema: String,
												 avroSchemas: Option[Map[String, String]],
                    _id: Option[BSONObjectID] = None) extends Model

