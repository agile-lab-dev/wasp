package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.catalog.builders.catalogservices

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.EntityApi
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.{CatalogCoordinates, EntityCatalogService}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils.MetastoreCatalogTableNameBuilder

import java.net.URL


class MockPlatformCatalogService extends EntityCatalogService {
  override def getEntityApi(coordinates: CatalogCoordinates): EntityApi = coordinates.name match {
    case "mock" => EntityApi(new URL("http://localhost:9999"))
    case "integrationTest" => EntityApi(new URL("http://host.docker.internal:9999"))
    case _ => throw new Exception("Entity not found")
  }
  override def getEntityTableName(coordinates: CatalogCoordinates): String = MetastoreCatalogTableNameBuilder.getTableName(coordinates);
}
