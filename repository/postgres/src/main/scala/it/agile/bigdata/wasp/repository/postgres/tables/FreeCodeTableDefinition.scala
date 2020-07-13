package it.agile.bigdata.wasp.repository.postgres.tables


import java.sql.ResultSet

import it.agilelab.bigdata.wasp.models.FreeCodeModel


object FreeCodeTableDefinition extends ModelTableDefinition[FreeCodeModel] {

  val tableName = "FREE_CODE"

  //val name = "name"
  val code = "code"

  val columns: Seq[String] = Seq(
    name,
    code
  )

  val ddl = s"""CREATE TABLE IF NOT EXISTS $tableName (
       |$name varchar NOT NULL,
       |$code varchar NOT NULL,
       |PRIMARY KEY ($name))
       |""".stripMargin

  override val from: ResultSet => FreeCodeModel = rs => FreeCodeModel(rs.getString(name),rs.getString(code))
  override val to: FreeCodeModel => Array[(String, Any)] = m=> Array((name,m.name),(code,m.code))
}
