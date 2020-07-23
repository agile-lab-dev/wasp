package it.agile.bigdata.wasp.repository.postgres

import it.agile.bigdata.wasp.repository.postgres.tables._
import java.sql.ResultSet
import it.agile.bigdata.wasp.repository.postgres.utils.PostgresDBHelper
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.repository.core.db.WaspDB





trait WaspPostgresDB extends WaspDB with PostgresDBHelper {

  def insert[T, K](obj: T)(implicit tableDefinition: TableDefinition[T, K]): Unit

  def getAll[T, K]()(implicit tableDefinition: TableDefinition[T, K]): Seq[T]

  def getBy[T, K](condition: Array[(String, Any)], sortCondition: Option[String] = None, limit: Option[Int] = None)(implicit tableDefinition: TableDefinition[T, K]): Seq[T]

  def getByPrimaryKey[T, K](primaryKey: K)(implicit tableDefinition: TableDefinition[T, K]): Option[T]

  def deleteByPrimaryKey[T, K](primaryKey: K)(implicit tableDefinition: TableDefinition[T, K]): Unit

  def close(): Unit

  def createTable()(implicit tableDefinition: TableDefinition[_, _]): Unit

  private[postgres] def dropTable()(implicit tableDefinition: TableDefinition[_,_]) : Unit

  def updateByPrimaryKey[T, K](obj: T)(implicit tableDefinition: TableDefinition[T, K]): Unit

  def upsert[T, K](obj: T)(implicit table: TableDefinition[T, K]): Unit

  def insertIfNotExists[T, K](obj: T)(implicit table: TableDefinition[T, K]): Unit

  def insertReturning[T, K, R](obj: T, columnResult: Array[String], mapperResultSet: ResultSet => R)(implicit table: TableDefinition[T, K]): Seq[R]

}

  object WaspPostgresDB extends Logging {


  val tableDefinitions = Seq(
    MlModelOnlyDataTableDefinition,
    MlModelOnlyInfoTableDefinition,
    BatchSchedulersTableDefinition,
    BatchJobTableDefinition,
    BatchJobInstanceTableDefinition,
    DocumentTableDefinition,
    FreeCodeTableDefinition,
    IndexTableDefinition,
    KeyValueTableDefinition,
    PipegraphTableDefinition,
    PipegraphInstanceTableDefinition,
    ProcessGroupTableDefinition,
    ProducerTableDefinition,
    RawTableDefinition,
    SqlSourceTableDefinition,
    TopicTableDefinition
  )


    var waspDB: WaspPostgresDB = _

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

    def createTables(waspDB: WaspPostgresDB):Unit = {
      waspDB.execute(tableDefinitions.map(_.ddl): _*)
    }


  }


