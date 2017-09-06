package it.agilelab.bigdata.wasp.consumers.spark.utils

/**
  * Created by Agile Lab s.r.l. on 05/09/2017.
  */
object Utils {

  def getIndexType(indexType: String, defaultDataStoreIndexed: String): String =  {
    if (indexType == "index") {
      defaultDataStoreIndexed
    } else {
      indexType
    }
  }
}
