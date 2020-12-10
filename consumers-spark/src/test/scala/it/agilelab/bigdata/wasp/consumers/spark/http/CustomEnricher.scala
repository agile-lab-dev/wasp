package it.agilelab.bigdata.wasp.consumers.spark.http

import it.agilelab.bigdata.wasp.consumers.spark.http.data.SampleData

import scala.collection.immutable.Map
import scala.reflect.ClassTag

class CustomEnricher extends Enricher {
  override def call[A: ClassTag, B: ClassTag](
                                               body: A,
                                               params: Map[String, String],
                                               headers: Map[String, String]
                                             ): B = {
    SampleData("abc123", "Text1").asInstanceOf[B]
  }

  override def close(): Unit = {}
}