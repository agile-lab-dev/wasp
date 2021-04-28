package it.agilelab.bigdata.microservicecatalog.tools.builders.catalogservices

import it.agilelab.bigdata.microservicecatalog.MicroserviceIdBuilder

object EntityIdBuilder extends MicroserviceIdBuilder {
  def buildId(microserviceDetails: Map[String, String]) = {
    if (!microserviceDetails.keySet.contains("name")) throw new Exception("A name is needed to build entity id")
    microserviceDetails.get("name").get
  }
}
