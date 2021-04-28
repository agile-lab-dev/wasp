package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.testentity

import it.agilelab.bigdata.microservicecatalog.MicroserviceCatalogService

object MockEntity extends EntitySDK("mock") {
  override def getBaseUrl(): String = "http://localhost:9999"
}

object IntegrationTestEntity extends EntitySDK("integrationTest") {
  override def getBaseUrl(): String = "http://host.docker.internal:9999"
}

class MockPlatformCatalogService extends MicroserviceCatalogService[EntitySDK](EntityIdBuilder) {
  override def getMicroservice(microserviceId: String): EntitySDK =  microserviceId match {
    case "mock" => MockEntity
    case "integrationTest" => IntegrationTestEntity
    case _ => throw new Exception("Entity not found")
  }
}
