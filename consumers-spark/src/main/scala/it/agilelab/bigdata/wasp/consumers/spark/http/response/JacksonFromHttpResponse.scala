package it.agilelab.bigdata.wasp.consumers.spark.http.response

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.http.HttpResponse
import org.apache.http.util.EntityUtils

import scala.reflect.ClassTag

class JacksonFromHttpResponse extends FromHttpResponse {

  private def deserialize[A: ClassTag](entity: String): A = {
    val mapper = new ObjectMapper
    mapper.registerModule(DefaultScalaModule)

    if (entity.nonEmpty) {
      mapper
        .readValue(
          entity,
          implicitly[ClassTag[A]]
            .runtimeClass
            .asInstanceOf[Class[A]]
        )
    } else {
      implicitly[ClassTag[A]]
        .runtimeClass
        .asInstanceOf[A]
    }
  }

  override def fromResponse[A: ClassTag](response: HttpResponse): A = {
    val entity = EntityUtils.toString(response.getEntity)
    deserialize[A](entity)
  }
}
