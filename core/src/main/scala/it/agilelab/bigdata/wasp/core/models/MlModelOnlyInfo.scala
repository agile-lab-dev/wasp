package it.agilelab.bigdata.wasp.core.models

import org.joda.time.DateTime
import reactivemongo.bson.BSONObjectID

case class MlModelOnlyInfo(name: String, version: String, className: Option[String] = None,
                               timestamp: Option[Long] = None, modelFileId: Option[BSONObjectID] = None,
                               favorite: Boolean = false, description: String = "", _id: Option[BSONObjectID] = None
                            ) extends Model