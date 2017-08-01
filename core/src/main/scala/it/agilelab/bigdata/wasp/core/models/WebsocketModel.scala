package it.agilelab.bigdata.wasp.core.models

import reactivemongo.bson.{BSONDocument, BSONObjectID}

case class WebsocketModel ( override val name: String,
                            host: String,
                            port: String,
                            resourceName: String,
                            //var isActive: Boolean,
                            options: Option[BSONDocument] = None,
                            _id: Option[BSONObjectID] = None) extends Model