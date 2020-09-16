package org.apache.spark

import java.net.URI

object UtilsForwarder {

  def resolveURI(path: String) : URI=
    org.apache.spark.util.Utils.resolveURI(path)

}
