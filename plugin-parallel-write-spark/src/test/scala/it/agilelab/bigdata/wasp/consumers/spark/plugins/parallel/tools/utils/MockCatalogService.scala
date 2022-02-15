package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.CatalogCoordinates
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.{DataCatalogService, GlueDataCatalogTableNameBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructField, StructType}

object MockCatalogService extends DataCatalogService{
  override def getSchema(sparkSession: SparkSession, entityCoordinates: CatalogCoordinates): StructType =
    StructType(Seq[StructField](
      StructField("column1", DataType.fromDDL("STRING")),
      StructField("column2", DataType.fromDDL("STRING"))))

  override def getPartitioningColumns(sparkSession: SparkSession, entityCoordinates: CatalogCoordinates): Seq[String] = Seq.empty

  override def getFullyQualifiedTableName(entityCoordinates: CatalogCoordinates): String =
    GlueDataCatalogTableNameBuilder.getTableName(entityCoordinates)
}
