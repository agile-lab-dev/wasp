package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.catalog.builders.mockbuilders

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog._

object RightMockBuilder extends EntityCatalogBuilder {
  override def getEntityCatalogService(): EntityCatalogService = {
    getEntityCatalogService("plugin.microservice-catalog.catalog-class")
  }
}
