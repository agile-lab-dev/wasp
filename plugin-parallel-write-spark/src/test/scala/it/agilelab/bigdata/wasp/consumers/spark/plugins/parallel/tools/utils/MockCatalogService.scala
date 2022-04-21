package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.CatalogCoordinates
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.DataCatalogService
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ DataType, StructField, StructType }

class MockCatalogService(schema: StructType) extends DataCatalogService with Serializable {
  override def getSchema(sparkSession: SparkSession, entityCoordinates: CatalogCoordinates): StructType = schema

  override def getPartitioningColumns(sparkSession: SparkSession, entityCoordinates: CatalogCoordinates): Seq[String] =
    Seq.empty

  override def getFullyQualifiedTableName(entityCoordinates: CatalogCoordinates): String =
    MetastoreCatalogTableNameBuilder.getTableName(entityCoordinates)
}

object MockCatalogService extends Serializable {
  def apply(): MockCatalogService =
    new MockCatalogService(
      StructType(
        Seq[StructField](
          StructField("column1", DataType.fromDDL("STRING")),
          StructField("column2", DataType.fromDDL("STRING"))
        )
      )
    )

  def apply(schema: StructType): MockCatalogService = new MockCatalogService(schema)

}
