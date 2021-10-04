package it.agilelab.bigdata.wasp.repository.core.dbModels

import it.agilelab.bigdata.wasp.models.Model

trait IndexDBModel extends Model

case class IndexDBModelV1(override val name: String,
                          creationTime: Long,
                          schema: Option[String],
                          query: Option[String] = None,
                          numShards: Option[Int] = Some(1),
                          replicationFactor: Option[Int] = Some(1),
                          rollingIndex: Boolean = true,
                          idField: Option[String] = None,
                          options: Map[String, String] = Map.empty
                         ) extends IndexDBModel
