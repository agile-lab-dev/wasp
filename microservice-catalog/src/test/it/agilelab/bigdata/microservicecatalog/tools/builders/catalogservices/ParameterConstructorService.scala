package it.agilelab.bigdata.microservicecatalog.tools.builders.catalogservices

import it.agilelab.bigdata.microservicecatalog.MicroserviceCatalogService
import it.agilelab.bigdata.microservicecatalog.tools.builders.microservices.{EntitySDK, WrongEntity}

class ParameterConstructorService(param: String) extends MicroserviceCatalogService[EntitySDK](EntityIdBuilder){
  override protected def getMicroservice(microserviceId: String): EntitySDK = microserviceId match {
    case "mock" => EntitySDK
    case _ => throw new Exception ("Entity not found")
  }
}
