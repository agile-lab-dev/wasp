package it.agilelab.bigdata.wasp.models.configuration

case class PostgresDBConfigModel(url: String,
                                 user: String,
                                 password: String,
                                 driver: String,
                                 poolSize : Int )
