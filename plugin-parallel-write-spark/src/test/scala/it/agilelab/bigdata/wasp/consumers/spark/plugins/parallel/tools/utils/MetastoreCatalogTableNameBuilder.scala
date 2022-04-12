package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.CatalogCoordinates

object MetastoreCatalogTableNameBuilder {
  def getTableName(entityDetails: CatalogCoordinates): String = {
    if (entityDetails.overrideDbName.nonEmpty) {
      s"${entityDetails.overrideDbName.get}.${entityDetails.name}_${entityDetails.version}".toLowerCase
    } else if (entityDetails.dbPrefix.nonEmpty) {
      s"${entityDetails.dbPrefix.get}_${entityDetails.domain}.${entityDetails.name}_${entityDetails.version}".toLowerCase
    } else {
      s"${entityDetails.domain}.${entityDetails.name}_${entityDetails.version}".toLowerCase
    }
  }
}
