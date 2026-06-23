package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.catalog.builders.catalogservices

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.EntityApi
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.{CatalogCoordinates, EntityCatalogService}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils.MetastoreCatalogTableNameBuilder

import java.net.URL

class MockPlatformCatalogService extends EntityCatalogService {
  private val port = System.getProperty("wasp.test.mock.server.port", "9999")

  override def getEntityApi(coordinates: CatalogCoordinates): EntityApi = coordinates.name match {
    case "mock"            => EntityApi(new URL(s"http://localhost:$port"))
    case "integrationTest" => EntityApi(new URL(s"http://host.docker.internal:$port"))
    case _                 => throw new Exception("Entity not found")
  }
  override def getEntityTableName(coordinates: CatalogCoordinates): String =
    MetastoreCatalogTableNameBuilder.getTableName(coordinates);
}
