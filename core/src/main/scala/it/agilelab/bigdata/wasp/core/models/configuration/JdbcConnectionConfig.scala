package it.agilelab.bigdata.wasp.core.models.configuration

import it.agilelab.bigdata.wasp.core.models.Model

case class JdbcConfigModel(
                          name: String,
                          connections: Map[String, JdbcConnectionConfig]
                          ) extends Model

case class JdbcConnectionConfig(
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
                          ) {
  def connectionString: String = s"$host:$port"
}

case class JdbcPartitioningInfo(
                                 partitionColumn: String,
                                 lowerBound: String,
                                 upperBound: String
                               )