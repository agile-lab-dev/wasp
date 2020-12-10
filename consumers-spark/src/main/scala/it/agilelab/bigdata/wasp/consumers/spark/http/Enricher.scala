package it.agilelab.bigdata.wasp.consumers.spark.http

import scala.reflect.ClassTag

trait Enricher extends AutoCloseable {
  def call[A: ClassTag, B: ClassTag](
                                      body: A,
                                      params: Map[String, String],
                                      headers: Map[String, String]
                                    ): B
}
