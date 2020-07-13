package it.agile.bigdata.wasp.repository.postgres


import it.agile.bigdata.wasp.repository.postgres.tables.TableDefinition
import it.agilelab.bigdata.wasp.models.Model
import it.agilelab.bigdata.wasp.models.configuration.PostgresDBConfigModel

import it.agilelab.bigdata.wasp.utils.JsonSupport

class WaspPostgresDBImpl(pgDBConf : PostgresDBConfigModel) extends WaspPostgresDB with JsonSupport{



  def insert[T <: Model,K](obj : T)(implicit tableDefinition: TableDefinition[T,K]): Unit = {
    insert(tableDefinition.tableName,obj)(tableDefinition.to)
  }


  def getAll[T <: Model,K]()(implicit tableDefinition: TableDefinition[T,K]): Seq[T] = {
    selectAll(tableDefinition.tableName, tableDefinition.columns.toArray, None)(tableDefinition.from)
  }

  def getByPrimaryKey[T <: Model,K](primaryKey : K)(implicit tableDefinition: TableDefinition[T,K]): Option[T] = {
    selectAll(tableDefinition.tableName,
      tableDefinition.columns.toArray,
      Some(tableDefinition.conditionPrimaryKey(primaryKey))
    )(tableDefinition.from)
      .headOption
  }

  def updateByPrimaryKey[T <: Model,K](obj : T)(implicit tableDefinition: TableDefinition[T,K]): Unit = {
    val condition = tableDefinition.conditionPrimaryKey(tableDefinition.primaryKeyFromObject(obj))
    updateBy(tableDefinition.tableName,obj,condition)(tableDefinition.to)
  }

  def deleteByPrimaryKey[T <: Model,K](primaryKey : K)(implicit tableDefinition: TableDefinition[T,K]): Unit = {
    delete(tableDefinition.tableName,
      Some(tableDefinition.conditionPrimaryKey(primaryKey))
    )
  }

  def upsert[T <: Model,K](obj : T)(implicit table: TableDefinition[T,K]): Unit = {
    if(getByPrimaryKey(table.primaryKeyFromObject(obj)).isDefined)
      updateBy(table.tableName,obj,table.conditionPrimaryKey(table.primaryKeyFromObject(obj)))(table.to)
    else insert(obj)
  }



  def createTable[T <: Model,K]()(implicit tableDefinition: TableDefinition[T,K]) : Unit = {
    super.execute(tableDefinition.ddl)
  }


  def close() : Unit = super.closePool()


  override protected def getUrl: String = pgDBConf.url

  override protected def getUser: String = pgDBConf.user

  override protected def getPassword: String = pgDBConf.password

  override protected def getDriver: String = pgDBConf.driver

  override protected def getPoolSize: Int = pgDBConf.poolSize

}
