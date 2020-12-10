package it.agilelab.bigdata.wasp.consumers.spark.http.request

import it.agilelab.bigdata.wasp.models.configuration.RestEnrichmentSource
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase

import scala.reflect.ClassTag

trait ToHttpRequest {
  def toRequest[A: ClassTag](
                              conf: RestEnrichmentSource,
                              body: A,
                              params: Map[String, String],
                              headers: Map[String, String]
                            ): HttpEntityEnclosingRequestBase
}
