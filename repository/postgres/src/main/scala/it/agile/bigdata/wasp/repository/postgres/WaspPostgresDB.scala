package it.agile.bigdata.wasp.repository.postgres

import it.agile.bigdata.wasp.repository.postgres.tables._
import it.agile.bigdata.wasp.repository.postgres.utils.PostgresDBHelper
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.models.Model
import it.agilelab.bigdata.wasp.repository.core.db.WaspDB





trait WaspPostgresDB extends WaspDB with PostgresDBHelper {

  def insert[T <: Model,K](obj: T)(implicit tableDefinition: TableDefinition[T,K]) : Unit

  def getAll[T <: Model,K]()(implicit tableDefinition: TableDefinition[T,K]) : Seq[T]

  def getBy[T <: Model,K](condition : String)(implicit tableDefinition: TableDefinition[T,K]): Seq[T]

  def getByPrimaryKey[T <: Model,K](primaryKey : K)(implicit tableDefinition: TableDefinition[T,K]): Option[T]

  def deleteByPrimaryKey[T <: Model,K](primaryKey : K)(implicit tableDefinition: TableDefinition[T,K]): Unit

  def close() : Unit

  def createTable()(implicit tableDefinition: TableDefinition[_,_]) : Unit

  def updateByPrimaryKey[T <: Model,K](obj : T)(implicit tableDefinition: TableDefinition[T,K]): Unit

  def upsert[T <: Model,K](obj : T)(implicit table: TableDefinition[T,K]): Unit

  def insertIfNotExists[T <: Model,K](obj : T)(implicit table: TableDefinition[T,K]): Unit

}






object WaspPostgresDB extends  Logging{


  val tableDefinitions = Seq(
    BatchSchedulersTableDefinition,
    TopicTableDefinition,
    ProducerTableDefinition,
    IndexTableDefinition,
    FreeCodeTableDefinition,
    SqlSourceTableDefinition,
    BatchJobTableDefinition,
    ProcessGroupTableDefinition,
    BatchJobInstanceTableDefinition,
    RawTableDefinition,
    KeyValueTableDefinition
  )


  var waspDB : WaspPostgresDB = _

  def getDB(): WaspPostgresDB = {
    if (waspDB == null) {
      val msg = "The waspDB was not initialized"
      logger.error(msg)
      throw new Exception(msg)
    }
    waspDB
  }




  def initializeDB(): WaspPostgresDB = {
    // MongoDB initialization
    val pgDBConfig = ConfigManager.getPostgresDBConfig
    logger.info(
      s"Create connection to Postgres: url ${pgDBConfig.url}"
    )
    waspDB = new WaspPostgresDBImpl(pgDBConfig)
    createTables(waspDB)
    waspDB
  }

  def createTables(waspDB: WaspPostgresDB): Unit = {
    waspDB.execute(tableDefinitions.map(_.ddl):_*)
  }


}
