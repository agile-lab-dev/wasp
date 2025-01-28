package it.agilelab.bigdata.wasp.consumers.spark.plugins.postgresql

import it.agilelab.bigdata.wasp.models.{SQLSinkModel, UpsertIgnoreExisting, UpsertUpdateExisting}
import org.apache.spark.sql.types.StructType

trait PostgreSQLUpsertGenerationSupport {

  def generateUpsertQuery(sqlSinkModel: SQLSinkModel, schema: StructType): String = {
    sqlSinkModel.writeMode match {
      case UpsertIgnoreExisting => generateInsertOnConflictDoNothing(sqlSinkModel, schema)
      case UpsertUpdateExisting => generateInserOnConflictDoUpdate(sqlSinkModel, schema)
    }
  }

  def generateInsertOnConflictDoNothing(model: SQLSinkModel, schema: StructType): String = {
    val valuesClause   = createValuesClause(schema.map(_.name))
    val conflictTarget = createColumnNamesList(model.primaryKeys.toSeq)
    s"""INSERT INTO ${model.table} AS ${model.tableAliasForExistingValues}
       |  $valuesClause
       |  ON CONFLICT $conflictTarget
       |  DO NOTHING
       |""".stripMargin
  }

  def generateInserOnConflictDoUpdate(model: SQLSinkModel, schema: StructType): String = {
    val conflictTarget = createColumnNamesList(model.primaryKeys.toSeq)
    val valuesClause   = createValuesClause(schema.map(_.name))
    val updates = createUpdates(model.updateClauses.get)
      .mkString(",\n    ") // concatenate with commas, insert newlines and whitespace for pretty print
    s"""INSERT INTO ${model.table} AS ${model.tableAliasForExistingValues}
       |  $valuesClause
       |  ON CONFLICT $conflictTarget
       |  DO UPDATE SET
       |    $updates
       |""".stripMargin
  }

  private def createUpdates(updateClauses: Map[String, String]): Seq[String] = {
    updateClauses.map { case (column, updateClause) => s"$column = $updateClause" }.toList
  }

  /** Create a column names list; given col1, col2, col3 returns "(col1, col2, col3)" */
  private def createColumnNamesList(columns: Seq[String]): String = columns.mkString("(", " , ", ")")

  /** Create a column names list; given col1, col2, col3 returns "(?, ?, ?)" */
  private def createPlaceholdersList(columns: Seq[String]): String = columns.map(_ => "?").mkString("(", " , ", ")")

  /** Create a values clause; given col1, col2, col3 returns "(col1, col2, col3) values (?, ?, ?)" */
  private def createValuesClause(columns: Seq[String]): String =
    s"${createColumnNamesList(columns)} values ${createPlaceholdersList(columns)}"

}
