package it.agilelab.bigdata.wasp.consumers.spark.utils

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.SQLDate

trait CompatibilityAvroSerializerExpression {

  self: AvroSerializerExpression =>
  def serializeTimestamp =
    (item: Any) =>
      DateTimeUtils.toJavaTimestamp(item.asInstanceOf[Long]).getTime

  def serializeDateType =
    (item: Any) =>
      if (item == null) null
      else
      {
    println("---item")
    println(item)
    println(item.getClass.getName)
    DateTimeUtils.daysToMillis(item.asInstanceOf[SQLDate], timeZone)
  }
}
