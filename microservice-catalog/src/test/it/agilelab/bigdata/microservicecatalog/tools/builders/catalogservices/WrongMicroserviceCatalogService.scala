package it.agilelab.bigdata.microservicecatalog.tools.builders.catalogservices

import it.agilelab.bigdata.microservicecatalog.MicroserviceCatalogService
import it.agilelab.bigdata.microservicecatalog.tools.builders.microservices.WrongEntity

class WrongMicroserviceCatalogService extends MicroserviceCatalogService[WrongEntity](EntityIdBuilder){
  override protected def getMicroservice(microserviceId: String): WrongEntity = microserviceId match {
    case "mock" => WrongEntity
    case _ => throw new Exception ("Entity not found")
  }
}
