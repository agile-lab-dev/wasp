package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.catalog

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.{Entity, ParallelWriteEntity}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.{CatalogCoordinates, MicroserviceCatalogService}

import java.net.URL

class EntityCatalog extends MicroserviceCatalogService {
  /**
    * Builds microservice id depending on microserviceDetails and retrieve microservice from catalog
    *
    * @param microserviceDetails Map containing microservice informations useful to id builder. Example: Map(("name", "microserviceName"), ("domain", "somedomain"))
    * @return Microservice instance
    */
  override def getMicroservice(coordinates: CatalogCoordinates): ParallelWriteEntity =
    Entity(new URL("http://localhost:9999/"))
}
