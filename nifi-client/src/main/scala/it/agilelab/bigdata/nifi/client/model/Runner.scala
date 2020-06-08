package it.agilelab.bigdata.nifi.client.model

import it.agilelab.bigdata.nifi.client.api.FlowApi
import it.agilelab.bigdata.nifi.client.core.SttpSerializer

object Runner {
  def main(args: Array[String]): Unit = {
    import sttp.client._
    implicit val sttpBackend = HttpURLConnectionBackend()
    implicit val s =  new SttpSerializer()
    println(FlowApi("http://localhost:8080/nifi-api").getRegistries().send())
  }
}
