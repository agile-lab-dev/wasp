package it.agilelab.bigdata.wasp.consumers.spark.http.response

import it.agilelab.bigdata.wasp.consumers.spark.http.data.SampleData
import org.apache.http.client.entity.EntityBuilder
import org.apache.http.{ProtocolVersion, StatusLine}
import org.apache.http.message.BasicHttpResponse
import org.scalatest.{FlatSpec, Matchers}

class JacksonFromHttpResponseTest extends FlatSpec with Matchers {

  it should "test JacksonFromHttpResponse" in {

    val response = new BasicHttpResponse(new StatusLine {
      override def getProtocolVersion: ProtocolVersion = new ProtocolVersion("HTTP",1 ,1)
      override def getStatusCode: Int = 200
      override def getReasonPhrase: String = "SampleData"
    })

    val responseBody = """{ "id": "abc123", "text": "Text1" }""".stripMargin

    response.setEntity(EntityBuilder.create().setText(responseBody).build())
    val jacksonFromHttpResponse = new JacksonFromHttpResponse
    val sampleData: SampleData = jacksonFromHttpResponse.fromResponse[SampleData](response)

    sampleData shouldBe SampleData("abc123", "Text1")
  }
}
