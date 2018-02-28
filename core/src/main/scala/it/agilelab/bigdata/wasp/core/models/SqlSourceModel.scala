package it.agilelab.bigdata.wasp.core.models

import it.agilelab.bigdata.wasp.core.models.configuration.JdbcPartitioningInfo

object SqlSourceModel{
  def readerType: String = "jdbc"
}
case class SqlSourceModel(
                           name: String,
                           connectionName: String,
                           database: String,
                           dbtable: String,
                           partitioningInfo: Option[JdbcPartitioningInfo],
                           numPartitions: Option[Int],
                           fetchSize: Option[Int]
                         ) extends Model
