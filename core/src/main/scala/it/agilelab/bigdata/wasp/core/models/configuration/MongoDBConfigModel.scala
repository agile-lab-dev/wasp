package it.agilelab.bigdata.wasp.core.models.configuration

case class MongoDBConfigModel(address: String,
                              databaseName: String,
                              username: String,
                              password: String,
                              millisecondsTimeoutConnection: Int)
