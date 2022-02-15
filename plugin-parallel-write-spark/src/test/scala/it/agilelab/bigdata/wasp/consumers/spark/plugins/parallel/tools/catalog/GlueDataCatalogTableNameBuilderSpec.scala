package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.catalog

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.CatalogCoordinates
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.GlueDataCatalogTableNameBuilder
import org.scalatest.FunSuite


class GlueDataCatalogTableNameBuilderSpec extends FunSuite {
  private val entityDetails: CatalogCoordinates = CatalogCoordinates("default", "mock", "v1")
  private val entityDetailsWithGlueDB: CatalogCoordinates = CatalogCoordinates("default", "mock", "v1", Some("mock_glue_db"))
  private val entityDetailsWithGlueDBOverride: CatalogCoordinates = CatalogCoordinates("default", "mock", "v1", None, Some("mock_glue_db"))

  test("Entity details without gluedb") {
    assert(GlueDataCatalogTableNameBuilder.getTableName(entityDetails) == "default.mock_v1")
  }

  test("Entity details with gluedb prefix") {
    assert(GlueDataCatalogTableNameBuilder.getTableName(entityDetailsWithGlueDB) == "mock_glue_db_default.mock_v1")
  }

  test("Entity details with gluedb override") {
    assert(GlueDataCatalogTableNameBuilder.getTableName(entityDetailsWithGlueDBOverride) == "mock_glue_db.mock_v1")
  }
}
