package it.agilelab.bigdata.microservicecatalog

/**
  * This class is used to get microservice from catalog
  * @param microserviceIdBuilder  Microservice id building logic
  * @tparam T                     Type of microservice
  */
abstract class MicroserviceCatalogService[T <: MicroserviceClient](microserviceIdBuilder: MicroserviceIdBuilder) {
  protected def getMicroservice(microserviceId: String): T

  /**
    * Builds microservice id depending on microserviceDetails and retrieve microservice from catalog
    * @param microserviceDetails Map containing microservice informations useful to id builder. Example: Map(("name", "microserviceName"), ("domain", "somedomain"))
    * @return Microservice instance
    */
  final def getMicroservice(microserviceDetails: Map[String, String]): T = getMicroservice(microserviceIdBuilder.buildId(microserviceDetails))
}


