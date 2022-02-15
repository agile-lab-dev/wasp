package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.CatalogCoordinates
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

trait DataCatalogService {

  def getSchema(sparkSession: SparkSession, entityCoordinates: CatalogCoordinates): StructType

  def getPartitioningColumns(sparkSession: SparkSession, entityCoordinates: CatalogCoordinates): Seq[String]

  def getFullyQualifiedTableName(entityCoordinates: CatalogCoordinates): String
}
