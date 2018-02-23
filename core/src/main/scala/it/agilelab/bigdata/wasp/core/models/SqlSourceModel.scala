package it.agilelab.bigdata.wasp.core.models

case class SqlSourceModel(
                           name: String,
                           database: String,
                           dbtable: String
                         ) extends Model
