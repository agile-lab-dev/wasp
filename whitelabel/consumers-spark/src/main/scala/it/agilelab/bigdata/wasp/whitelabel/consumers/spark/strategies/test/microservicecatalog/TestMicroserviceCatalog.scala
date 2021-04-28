package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.microservicecatalog

import it.agilelab.bigdata.microservicecatalog.MicroserviceCatalogService

object IntegrationTestEntity extends EntitySDK("integrationTest") {
  override def getBaseUrl(): String = "http://host.docker.internal:9999"
}

class TestMicroserviceCatalog extends MicroserviceCatalogService[EntitySDK](EntityIdBuilder) {
  override def getMicroservice(microserviceId: String): EntitySDK = microserviceId match {
    case "integrationTest" => IntegrationTestEntity
    case _ => throw new Exception("Entity not found")
  }
}