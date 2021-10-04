package it.agilelab.bigdata.wasp.repository.core.dbModels

import it.agilelab.bigdata.wasp.models.Model

trait MultiTopicDBModel extends Model

case class MultiTopicDBModelV1(override val name: String,
                                    topicNameField: String,
                                    topicModelNames: Seq[String]
                                   ) extends MultiTopicDBModel
