package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.{CatalogCoordinates, EntityCatalogBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.types.StructType

import java.util.concurrent.ConcurrentHashMap
import java.util.function
import scala.util.{Failure, Success, Try}

object MetastoreCatalogService extends DataCatalogService with Serializable {
  private lazy val catalogCache: ConcurrentHashMap[String, CatalogTable] = new ConcurrentHashMap[String, CatalogTable]()

  def getSchema(sparkSession: SparkSession, entityCoordinates: CatalogCoordinates): StructType =
    getTable(sparkSession, getFullyQualifiedTableName(entityCoordinates)).schema

  def getPartitioningColumns(sparkSession: SparkSession, entityCoordinates: CatalogCoordinates): Seq[String] =
    getTable(sparkSession, getFullyQualifiedTableName(entityCoordinates)).partitionColumnNames

  /**
    * Retrieve metadata for an external table
    */
  private def getTable(spark: SparkSession, tableName: String): CatalogTable = {
    catalogCache.computeIfAbsent(tableName, new function.Function[String, CatalogTable] {
      override def apply(t: String): CatalogTable = getTableFromCatalog(spark, tableName)
    })
  }

  private def getTableFromCatalog(spark: SparkSession, tableName: String): CatalogTable = {
    Try(spark.catalog.getTable(tableName)) match {
      case Success(genericTable) =>
        spark.sessionState.catalog.externalCatalog.getTable(genericTable.database, genericTable.name)
      case Failure(ex: Throwable) =>
        throw new Exception(s"Unable to get table with name ${tableName}: ${ex.getMessage}")
    }
  }

  override def getFullyQualifiedTableName(entityDetails: CatalogCoordinates): String =
    EntityCatalogBuilder.getEntityCatalogService().getEntityTableName(entityDetails)
}
