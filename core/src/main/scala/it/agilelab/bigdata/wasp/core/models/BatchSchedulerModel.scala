package it.agilelab.bigdata.wasp.core.models

import reactivemongo.bson.{BSONDocument, BSONObjectID}

case class BatchSchedulerModel (override val name: String,
                                quartzUri: String,
                                batchJob: Option[BSONObjectID],
                                options: Option[BSONDocument] = None,
                                isActive: Boolean = true,
                                _id: Option[BSONObjectID] = None
                                 ) extends Model