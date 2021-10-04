package it.agilelab.bigdata.wasp.repository.core.dbModels

import it.agilelab.bigdata.wasp.models.Model
import it.agilelab.bigdata.wasp.repository.core.mappers.MlDBModelMapperV1
import org.mongodb.scala.bson.BsonObjectId

trait MlDBModelOnlyInfo extends Model

case class MlDBModelOnlyInfoV1(name: String,
                              version: String = MlDBModelMapperV1.version,
                              className: Option[String] = None,
                              timestamp: Option[Long] = None,
                              modelFileId: Option[BsonObjectId] = None,
                              favorite: Boolean = false,
                              description: String = ""
                              ) extends MlDBModelOnlyInfo