package it.agile.bigdata.wasp.repository.postgres.tables

import java.sql.ResultSet


trait TableDefinition[T,K] {

  val tableName : String

  val columns: Seq[String]

  val ddl : String

  val from : ResultSet => T

  val to : T=> Array[(String,Any)]

  val conditionPrimaryKey : K => String

  val primaryKeyFromObject : T => K


}

