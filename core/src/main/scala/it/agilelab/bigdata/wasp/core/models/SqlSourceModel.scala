package it.agilelab.bigdata.wasp.core.models

object SqlSourceModel{
  def readerType: String = "jdbc"
}
case class SqlSourceModel(
                           name: String,
                           connectionName: String,
                           database: String,
                           dbtable: String
                         ) extends Model
