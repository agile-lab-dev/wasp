package it.agilelab.bigdata.wasp.models

import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.PostgreSQLProduct

/** Model for an SQL sink. Currently targeted at upserts only.
  *
  * @param name
  *   Name of the model
  * @param table
  *   Table name to upsert into
  * @param tableAliasForExistingValues
  *   Table alias to use for providing access to existing values in the updates clause
  * @param primaryKeys
  *   Primary key columns
  * @param writeMode
  *   Write mode (insert/upsert/update/delete type)
  * @param updateClauses
  *   Update clauses to use when performing the update; key is the column name, value is the clause
  * @param dialect
  *   SQL dialect for the upsert query
  * @param jdbcConnection
  *   JDBC connection information
  * @param batchSize
  *   Size of the batches to use when writing
  * @param poolSize
  *   Connection pool size; scale according to executor cores
  */
case class SQLSinkModel(
    name: String,
    table: String,
    tableAliasForExistingValues: String,
    primaryKeys: List[String],
    writeMode: WriteMode,
    updateClauses: Option[Map[String, String]],
    dialect: Dialect,
    jdbcConnection: JDBCConnection,
    batchSize: Int,
    poolSize: Int
) extends DatastoreModel {
  require(primaryKeys.nonEmpty, "A primary key must be specified")
  require(primaryKeys.size == primaryKeys.toSet.size, "The primary key cannot contain duplicate column names")
  require(
    writeMode == UpsertIgnoreExisting && updateClauses.isEmpty || writeMode == UpsertUpdateExisting && updateClauses.isDefined,
    "Update clauses must be specified if (and only if) upsert mode is not \"ignore\"."
  )
  require(batchSize > 0, "Batch size must be > 0")
  require(poolSize > 0, "Pool size must be > 0")

  override def datastoreProduct: DatastoreProduct = PostgreSQLProduct
}

case class JDBCConnection(
    name: String,
    url: String, // JDBC Connection String
    user: String,
    password: String,
    driverName: String,
    properties: Option[Map[String, String]]
) {
  require(
    checkKeyNotInProperties("user") && checkKeyNotInProperties("password"),
    "User and password cannot be specified in the properties"
  )

  private def checkKeyNotInProperties(key: String): Boolean = properties.map(_.get(key)).isEmpty
}

// TODO: must improve in order to add SQL dialects, this applies well to PostgreSQL, not so much to SQL's standard MERGE
sealed abstract class WriteMode
case object UpsertIgnoreExisting extends WriteMode
case object UpsertUpdateExisting extends WriteMode
object WriteMode {
  // TODO improve, use decent enumeration
  val upsertIgnoreExisting = "UpsertIgnoreExisting"
  val upsertUpdateExisting = "UpsertUpdateExisting"
}

// TODO either rename or move to dedicated package, object PostgreSQL is confusing
sealed abstract class Dialect
case object PostgreSQL extends Dialect
object Dialect {
  // TODO improve, use decent enumeration
  val postgreSQL = "PostgreSQL"
}
