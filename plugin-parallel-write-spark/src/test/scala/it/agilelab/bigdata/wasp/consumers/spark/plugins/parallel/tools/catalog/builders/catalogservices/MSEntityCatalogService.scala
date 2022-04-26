package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.catalog.builders.catalogservices

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.{EntityApi, ParallelWriteEntity}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.{CatalogCoordinates, EntityCatalogService}

import java.net.URL

class MSEntityCatalogService extends EntityCatalogService{
  /**
    * Builds microservice id depending on microserviceDetails and retrieve microservice from catalog
    *
    * @param microserviceDetails Map containing microservice informations useful to id builder. Example: Map(("name", "microserviceName"), ("domain", "somedomain"))
    * @return Microservice instance
    */
  override def getEntityApi(coordinates: CatalogCoordinates): ParallelWriteEntity = {
    EntityApi(new URL("http://ingress/http/entity/batch/goofyhot/v1"))
  }

  override def getEntityTableName(coordinates: CatalogCoordinates): String = "goofyhot_v1"
}
