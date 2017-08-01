package it.agilelab.bigdata.wasp.core.utils

import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar
import java.sql.Time

object TimeFormatter {

  val baseFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

  def format(time: Date): String = new SimpleDateFormat(baseFormat).format(time)

  def format(time: String, format: String): String = this.format(new SimpleDateFormat(format).parse(time))

  def format(time: Calendar): String = new SimpleDateFormat(baseFormat).format(time.getTime)

  def format(time: Time): String = new SimpleDateFormat(baseFormat).format(time.getTime)

  def formatFromBaseToUDF(time: String, format: String) = new SimpleDateFormat(format).format(new SimpleDateFormat(baseFormat).parse(time))
}