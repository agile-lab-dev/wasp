package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.catalog.builders.mockbuilders

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog._


object WrongCatalogMockBuilder extends MicroserviceCatalogBuilder {
  override def getMicroserviceCatalogService(): MicroserviceCatalogService = {
    getMicroserviceCatalogService("plugin.microservice-catalog-wrongcatalog.catalog-class")
  }
}