package it.agilelab.bigdata.microservicecatalog.tools.builders.catalogservices

import it.agilelab.bigdata.microservicecatalog.MicroserviceCatalogService
import it.agilelab.bigdata.microservicecatalog.tools.builders.microservices.EntitySDK



class MockPlatformCatalogService extends MicroserviceCatalogService[EntitySDK](EntityIdBuilder) {
  override def getMicroservice(microserviceId: String): EntitySDK =  microserviceId match {
    case "mock" => EntitySDK
    case _ => throw new Exception("Entity not found")
  }
}
