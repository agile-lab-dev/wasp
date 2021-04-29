package it.agilelab.bigdata.wasp.consumers.spark.plugins.continuous.tools

import com.squareup.okhttp.mockwebserver.MockResponse

trait WebAssertion {
  val holds: Boolean
  val message: String
  def toResponse: MockResponse = if (holds) {
    new MockResponse().setResponseCode(200)
  } else {
    new MockResponse().setResponseCode(500).setBody(message)
  }

  override def toString: String = if (holds) {
    "Assertion successful"
  } else {
    s"Assertion failed: $message"
  }
}

case class AggregatedAssertion(assertions: List[WebAssertion]) extends WebAssertion {
  override val holds: Boolean  = assertions.forall(_.holds)
  override val message: String = assertions.filterNot(_.holds).map(_.message).mkString("\n")
}

object AggregatedAssertion {
  def apply(assertions: WebAssertion*): AggregatedAssertion = AggregatedAssertion(assertions.toList)
}

case class EqualAssertion[A](expected: A, actual: A, messageFormat: String = "Expected: %s got %s")
    extends WebAssertion {
  override val holds: Boolean  = expected == actual
  override val message: String = messageFormat.format(expected, actual)
}
