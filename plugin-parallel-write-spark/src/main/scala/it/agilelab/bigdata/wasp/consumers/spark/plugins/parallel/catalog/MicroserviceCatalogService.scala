package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.ParallelWriteEntity

/**
  * Coordinates needed by entity catalog and data catalog
  *
  * @param domain   entity domain
  * @param name     entity name
  * @param version  entity version
  */
case class CatalogCoordinates(domain: String, name: String, version: String, glueDbPrefix: Option[String] = None, overrideGlueDbName: Option[String] = None)

/**
  * This class is used to get microservice from catalog
  * @param microserviceIdBuilder  Microservice id building logic
  * @tparam T                     Type of microservice
  */
abstract class MicroserviceCatalogService {

  /**
    * Builds microservice id depending on microserviceDetails and retrieve microservice from catalog
    * @param microserviceDetails Map containing microservice informations useful to id builder. Example: Map(("name", "microserviceName"), ("domain", "somedomain"))
    * @return Microservice instance
    */
  def getMicroservice(coordinates: CatalogCoordinates): ParallelWriteEntity
}


