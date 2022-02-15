package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils

import java.net.URI

object HadoopS3Utils {
  def useS3aScheme(writeUri: URI): URI = {
    if (writeUri.getScheme == "s3")
      new URI("s3a", writeUri.getUserInfo, writeUri.getHost, writeUri.getPort,
        writeUri.getPath, writeUri.getQuery, writeUri.getFragment)
    else writeUri
  }
}
