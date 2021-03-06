package it.agilelab.bigdata.microservicecatalog.tools.builders.mockbuilders

import it.agilelab.bigdata.microservicecatalog.{MicroserviceCatalogBuilder, MicroserviceCatalogService, MicroserviceClient}

object ParameterBuilder extends MicroserviceCatalogBuilder {
  override def getMicroserviceCatalogService[T <: MicroserviceClient](): MicroserviceCatalogService[T] = {
    getMicroserviceCatalogService("plugin.microservice-catalog-constructorservice.catalog-class")
  }
}
