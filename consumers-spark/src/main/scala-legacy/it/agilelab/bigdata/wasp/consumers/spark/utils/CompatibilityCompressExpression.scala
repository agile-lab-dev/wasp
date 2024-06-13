package it.agilelab.bigdata.wasp.consumers.spark.utils

import org.apache.spark.sql.catalyst.expressions.Expression

trait CompatibilityCompressExpression {
  self: CompressExpression =>
  override def child: Expression = child

}