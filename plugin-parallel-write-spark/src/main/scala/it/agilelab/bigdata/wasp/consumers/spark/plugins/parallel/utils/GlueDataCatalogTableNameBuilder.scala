package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.CatalogCoordinates

object GlueDataCatalogTableNameBuilder {
  def getTableName(entityDetails: CatalogCoordinates): String = {
    if (entityDetails.overrideGlueDbName.nonEmpty) {
      s"${entityDetails.overrideGlueDbName.get}.${entityDetails.name}_${entityDetails.version}"
    } else if (entityDetails.glueDbPrefix.nonEmpty) {
      s"${entityDetails.glueDbPrefix.get}_${entityDetails.domain}.${entityDetails.name}_${entityDetails.version}"
    } else {
      s"${entityDetails.domain}.${entityDetails.name}_${entityDetails.version}"
    }
  }
}
