package it.agile.bigdata.wasp.repository.postgres


import java.sql.ResultSet

import it.agile.bigdata.wasp.repository.postgres.tables.TableDefinition
import it.agilelab.bigdata.wasp.models.configuration.PostgresDBConfigModel
import it.agilelab.bigdata.wasp.utils.JsonSupport

class WaspPostgresDBImpl(pgDBConf : PostgresDBConfigModel) extends WaspPostgresDB with JsonSupport{



  def insert[T,K](obj : T)(implicit tableDefinition: TableDefinition[T,K]): Unit = {
    insert(tableDefinition.tableName,obj)(tableDefinition.to)
  }


  def getBy[T,K](condition : Array[(String,Any)], sortCondition : Option[String]=None, limit :Option[Int]=None)(implicit tableDefinition: TableDefinition[T,K]): Seq[T] = {
    selectAll(tableDefinition.tableName, tableDefinition.columns.toArray, Some(condition),sortCondition,limit)(tableDefinition.from)
  }

  def getAll[T,K]()(implicit tableDefinition: TableDefinition[T,K]): Seq[T] = {
    selectAll(tableDefinition.tableName, tableDefinition.columns.toArray, None)(tableDefinition.from)
  }

  def getByPrimaryKey[T,K](primaryKey : K)(implicit tableDefinition: TableDefinition[T,K]): Option[T] = {
    selectAll(tableDefinition.tableName,
      tableDefinition.columns.toArray,
      Some(tableDefinition.conditionPrimaryKey(primaryKey))
    )(tableDefinition.from)
      .headOption
  }

  def updateByPrimaryKey[T,K](obj : T)(implicit tableDefinition: TableDefinition[T,K]): Unit = {
    val condition = tableDefinition.conditionPrimaryKey(tableDefinition.primaryKeyFromObject(obj))
    updateBy(tableDefinition.tableName,obj,condition)(tableDefinition.to)
  }

  def deleteByPrimaryKey[T,K](primaryKey : K)(implicit tableDefinition: TableDefinition[T,K]): Unit = {
    delete(tableDefinition.tableName,
      Some(tableDefinition.conditionPrimaryKey(primaryKey))
    )
  }

  def upsert[T,K](obj : T)(implicit table: TableDefinition[T,K]): Unit = {
    if(getByPrimaryKey(table.primaryKeyFromObject(obj)).isDefined)
      updateBy(table.tableName,obj,table.conditionPrimaryKey(table.primaryKeyFromObject(obj)))(table.to)
    else insert(obj)
  }

  def insertIfNotExists[T,K](obj : T)(implicit table: TableDefinition[T,K]): Unit = {
    if(getByPrimaryKey(table.primaryKeyFromObject(obj)).isEmpty) insert(obj)
  }



  def createTable()(implicit tableDefinition: TableDefinition[_,_]) : Unit = {
    super.execute(tableDefinition.ddl)
  }

  private[postgres] def dropTable()(implicit tableDefinition: TableDefinition[_,_]) : Unit = {
    dropTable(tableDefinition.tableName)
  }

  def insertReturning[T,K,R](obj : T,columnResult: Array[String], mapperResultSet : ResultSet=> R)(implicit table: TableDefinition[T,K]) : Seq[R] = {
    super.insertReturning(table.tableName,obj,columnResult)(table.to,mapperResultSet)
  }


  def close() : Unit = super.closePool()


  override protected def getUrl: String = pgDBConf.url

  override protected def getUser: String = pgDBConf.user

  override protected def getPassword: String = pgDBConf.password

  override protected def getDriver: String = pgDBConf.driver

  override protected def getPoolSize: Int = pgDBConf.poolSize

}
