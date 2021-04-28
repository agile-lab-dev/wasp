package it.agilelab.bigdata.wasp.consumers.spark.plugins.continuous.tools.testentity

import it.agilelab.bigdata.microservicecatalog.MicroserviceIdBuilder

object EntityIdBuilder extends MicroserviceIdBuilder {
  def buildId(microserviceDetails: Map[String, String]) = {
    if (!microserviceDetails.keySet.contains("name")) throw new Exception("A name is needed to build entity id")
    microserviceDetails.get("name").get
  }
}
