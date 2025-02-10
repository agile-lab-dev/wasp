package it.agilelab.bigdata.wasp.consumers.spark.utils

import org.apache.spark.sql.catalyst.expressions.Expression

trait CompatibilityEncodeUsingAvro[A] {
  self: EncodeUsingAvro[A] =>
  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}
