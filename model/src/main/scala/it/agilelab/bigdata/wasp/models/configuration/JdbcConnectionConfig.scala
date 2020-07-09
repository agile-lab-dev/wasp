package it.agilelab.bigdata.wasp.models.configuration

import it.agilelab.bigdata.wasp.models.Model

case class JdbcConfigModel(
                            connections: Map[String, JdbcConnectionConfig],
                            name: String
                          ) extends Model

case class JdbcConnectionConfig(
                                name: String,
                                url: String,  // JDBC Connection String
                                user: String,
                                password: String,
                                driverName: String
                              )

case class JdbcPartitioningInfo(
                                 partitionColumn: String,
                                 lowerBound: String,
                                 upperBound: String
                               )