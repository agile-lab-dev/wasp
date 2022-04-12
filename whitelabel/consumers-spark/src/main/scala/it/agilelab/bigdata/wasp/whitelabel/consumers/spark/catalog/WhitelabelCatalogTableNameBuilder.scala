package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.catalog

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.CatalogCoordinates

object WhitelabelCatalogTableNameBuilder {
  def getTableName(entityDetails: CatalogCoordinates): String = {
    if (entityDetails.overrideDbName.nonEmpty) {
      s"${entityDetails.overrideDbName.get}.${entityDetails.name}_${entityDetails.version}"
    } else if (entityDetails.dbPrefix.nonEmpty) {
      s"${entityDetails.dbPrefix.get}_${entityDetails.domain}.${entityDetails.name}_${entityDetails.version}"
    } else {
      s"${entityDetails.domain}.${entityDetails.name}_${entityDetails.version}"
    }
  }
}
