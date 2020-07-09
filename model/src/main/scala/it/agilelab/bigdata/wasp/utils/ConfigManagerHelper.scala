package it.agilelab.bigdata.wasp.utils

import java.text.SimpleDateFormat
import java.util.Date

object ConfigManagerHelper {
  def buildTimedName(prefix: String): String = {
    val result = prefix + "-" + new SimpleDateFormat("yyyy.MM.dd")
      .format(new Date())

    result
  }
}
