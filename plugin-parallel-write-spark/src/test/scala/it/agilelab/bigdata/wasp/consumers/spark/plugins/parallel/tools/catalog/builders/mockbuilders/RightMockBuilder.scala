package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.catalog.builders.mockbuilders

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog._

object RightMockBuilder extends MicroserviceCatalogBuilder {
  override def getMicroserviceCatalogService(): MicroserviceCatalogService = {
    getMicroserviceCatalogService("plugin.microservice-catalog.catalog-class")
  }
}
