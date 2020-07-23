package it.agilelab.bigdata.wasp.repository.postgres.tables

import java.sql.ResultSet


trait TableDefinition[T,K] {

  def tableName : String

  def columns: List[String]

  def ddl : String

  def from : ResultSet => T

  def to : T=> Array[(String,Any)]

  def conditionPrimaryKey : K => Array[(String,Any)]

  def primaryKeyFromObject : T => K


}

