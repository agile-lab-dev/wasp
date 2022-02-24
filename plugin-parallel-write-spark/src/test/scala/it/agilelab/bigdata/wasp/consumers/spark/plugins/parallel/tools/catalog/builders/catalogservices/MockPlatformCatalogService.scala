package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.catalog.builders.catalogservices

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.Entity
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.{CatalogCoordinates, MicroserviceCatalogService, entity}

import java.net.URL


class MockPlatformCatalogService extends MicroserviceCatalogService {
  override def getMicroservice(coordinates: CatalogCoordinates): Entity = coordinates.name match {
    case "mock" => entity.Entity(new URL("http://localhost:9999"))
    case "integrationTest" => entity.Entity(new URL("http://host.docker.internal:9999"))
    case _ => throw new Exception("Entity not found")
  }
}
