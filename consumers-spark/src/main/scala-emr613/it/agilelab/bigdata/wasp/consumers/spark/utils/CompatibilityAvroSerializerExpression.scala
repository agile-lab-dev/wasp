package it.agilelab.bigdata.wasp.consumers.spark.utils

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.DateTimeUtils

trait CompatibilityAvroSerializerExpression {
    self: AvroSerializerExpression =>
    override protected def withNewChildInternal(newChild: Expression): Expression = {
      copy(child = newChild)
    }

  def serializeTimestamp =
    (item: Any) =>
      DateTimeUtils.toJavaTimestamp(item.asInstanceOf[Long]).getTime
  def serializeDateType =
    (item: Any) =>
      if (item == null) null
      else {
        println("---item")
        println(item)
        println(item.getClass.getName)
        DateTimeUtils.toJavaDate(item.asInstanceOf[Int]).getTime
    }

}
