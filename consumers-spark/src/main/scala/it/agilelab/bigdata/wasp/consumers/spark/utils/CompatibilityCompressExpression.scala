package it.agilelab.bigdata.wasp.consumers.spark.utils

import org.apache.spark.sql.catalyst.expressions.Expression

trait CompatibilityCompressExpression {
  self: CompressExpression =>
  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}
