package it.agilelab.bigdata.wasp.models.configuration

case class MongoDBConfigModel(address: String,
                              databaseName: String,
                              username: String,
                              password: String,
                              millisecondsTimeoutConnection: Int,
                              collectionPrefix: String)
