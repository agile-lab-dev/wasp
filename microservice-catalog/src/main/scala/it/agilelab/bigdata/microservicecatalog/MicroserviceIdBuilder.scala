package it.agilelab.bigdata.microservicecatalog

/**
  * Each microservice should be identified by an ID used by MicroserviceCatalogService to retrieve the microservice from catalog
  * The logic used to build the ID is custom and defined by
  */
trait MicroserviceIdBuilder {
  /**
    * Builds microservice ID
    * @param microserviceDetails Details of microservice used to build id.
    * @return Microservice ID built according to entityDetails
    */
  def buildId(microserviceDetails: Map[String, String]): String
}
