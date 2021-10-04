package it.agilelab.bigdata.wasp.repository.core.dbModels

import it.agilelab.bigdata.wasp.models.Model
import it.agilelab.bigdata.wasp.models.configuration.JdbcPartitioningInfo

trait SqlSourceDBModel extends Model

case class SqlSourceDBModelV1(name: String,
                              connectionName: String,
                              dbtable: String,
                              partitioningInfo: Option[JdbcPartitioningInfo],
                              numPartitions: Option[Int],
                              fetchSize: Option[Int]
                             ) extends SqlSourceDBModel
