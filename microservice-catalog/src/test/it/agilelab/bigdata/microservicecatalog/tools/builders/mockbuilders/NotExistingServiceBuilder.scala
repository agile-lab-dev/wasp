package it.agilelab.bigdata.microservicecatalog.tools.builders.mockbuilders

import it.agilelab.bigdata.microservicecatalog.{MicroserviceCatalogBuilder, MicroserviceCatalogService, MicroserviceClient}

object NotExistingServiceBuilder extends MicroserviceCatalogBuilder {
  override def getMicroserviceCatalogService[T <: MicroserviceClient](): MicroserviceCatalogService[T] = {
    getMicroserviceCatalogService("plugin.microservice-catalog-notexistingservice.catalog-class")
  }
}
