package it.agilelab.bigdata.wasp.repository.core.dbModels

import it.agilelab.bigdata.wasp.models.Model

sealed trait ProducerDBModel extends Model {
  //  val version: String
}

case class ProducerDBModelV1(name: String,
                             className: String,
                             topicName: Option[String],
                             var isActive: Boolean = false,
                             configuration: Option[String] = None,
                             isRemote: Boolean,
                             isSystem: Boolean
                            ) extends ProducerDBModel

case class ProducerDBModelV2(name: String,
                             className: String,
                             topicName: Option[String],
                             var isActive: Boolean = false,
                             configuration: Option[String] = None
                            ) extends ProducerDBModel
