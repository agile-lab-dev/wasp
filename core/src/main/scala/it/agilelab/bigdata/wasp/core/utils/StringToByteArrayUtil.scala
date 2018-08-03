package it.agilelab.bigdata.wasp.core.utils

import java.io.UnsupportedEncodingException

/**
  * Created by matteo on 21/10/16.
  */
object StringToByteArrayUtil {

  def stringToByteArray(data: String): Array[Byte] = {
    val opData: Option[String] = Option(data)
    try {
      opData.map(_.getBytes("UTF-8")).orNull
    } catch {
      case e: UnsupportedEncodingException =>
        throw new UnsupportedEncodingException("Error when serializing String to Array[Byte] due to unsupported encoding UTF8")
    }
  }

  def byteArrayToString(binary: Array[Byte]): String = {
    try {
      new String(binary, "UTF-8")
    } catch {
      case e: UnsupportedEncodingException =>
        throw new UnsupportedEncodingException("Error when deserializing Array[Byte] to String due to unsupported encoding UTF-8");
    }
  }

}
