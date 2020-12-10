package it.agilelab.bigdata.wasp.consumers.spark.http.response

import org.apache.http.HttpResponse

import scala.reflect.ClassTag

trait FromHttpResponse {
  def fromResponse[A: ClassTag](response: HttpResponse): A
}
