package it.agilelab.bigdata.wasp.core.models.configuration

import it.agilelab.bigdata.wasp.core.models.Model

case class JdbcConfigModel(
                            name: String,
                            dbType: String,
                            host: String,
                            port: Int,
                            user: String,
                            password: String,
                            driverName: String,
                            partitioningInfo: Option[JdbcPartitioningInfo],
                            numPartitions: Option[Int],
                            fetchSize: Option[Int]
                          ) extends Model {
  def connectionString: String = s"$host:$port"
}

case class JdbcPartitioningInfo(
                                 partitionColumn: String,
                                 lowerBound: String,
                                 upperBound: String
                               )