package it.agilelab.bigdata.wasp.core.models

import it.agilelab.bigdata.wasp.core.models.configuration.JdbcPartitioningInfo

object SqlSourceModel{
  def readerType: String = Datastores.databaseCategory
}

/**
  * Class representing a SqlSource model
  *
  * @param name             The name of the SqlSource model
  * @param connectionName   The name of the connection to use.
  *                         N.B. have to be present in jdbc-subConfig
  * @param database         The name of the database.
  *                         N.B. just useful to use it in Strategy - not used to create the url (JDBC connection string) which is retrieved from jdbc-subConfig
  * @param dbtable          The name of the table
  * @param partitioningInfo optional - Partition info (column, lowerBound, upperBound)
  * @param numPartitions    optional - Number of partitions
  * @param fetchSize        optional - Fetch size
  */
case class SqlSourceModel(
                           name: String,
                           connectionName: String,
                           database: String,
                           dbtable: String,
                           partitioningInfo: Option[JdbcPartitioningInfo],
                           numPartitions: Option[Int],
                           fetchSize: Option[Int]
                         ) extends Model