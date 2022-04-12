package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.catalog.builders.mockbuilders

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog._

object WrongConfigurationPathBuilder extends EntityCatalogBuilder {
  override def getEntityCatalogService(): EntityCatalogService = {
    getEntityCatalogService("plugin.microservice-catalog.not-existing-path")
  }
}
