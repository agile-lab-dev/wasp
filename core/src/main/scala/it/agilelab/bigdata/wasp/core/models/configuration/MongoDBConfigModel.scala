package it.agilelab.bigdata.wasp.core.models.configuration

case class MongoDBConfigModel(address: String,
                              databaseName: String,
                              secondsTimeoutConnection: Int)
