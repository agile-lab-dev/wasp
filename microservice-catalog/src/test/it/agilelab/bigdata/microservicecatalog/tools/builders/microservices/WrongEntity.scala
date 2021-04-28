package it.agilelab.bigdata.microservicecatalog.tools.builders.microservices

import it.agilelab.bigdata.microservicecatalog.MicroserviceClient

abstract class WrongEntity extends MicroserviceClient
object WrongEntity extends WrongEntity {
  override def getBaseUrl(): String = "http://localhost:9999"
}
