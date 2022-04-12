package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.ParallelWriteEntity

/**
  * Coordinates needed by entity catalog and data catalog
  *
  * @param domain   entity domain
  * @param name     entity name
  * @param version  entity version
  */
case class CatalogCoordinates(domain: String, name: String, version: String, dbPrefix: Option[String] = None, overrideDbName: Option[String] = None)

/**
  * This class is used to get microservice from catalog
  */
abstract class EntityCatalogService {

  /**
    * Builds microservice id depending on microserviceDetails and retrieve microservice from catalog
    * @return Microservice instance
    */
  def getEntity(coordinates: CatalogCoordinates): ParallelWriteEntity
  def getEntityTableName(coordinates: CatalogCoordinates): String
}


