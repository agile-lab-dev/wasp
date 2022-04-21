package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.catalog.builders.catalogservices

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.EntityApi
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.{CatalogCoordinates, EntityCatalogService, entity}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils.MetastoreCatalogTableNameBuilder

import java.net.URL

class ParameterConstructorService(param: String) extends EntityCatalogService {
  override def getEntityApi(coordinates: CatalogCoordinates): EntityApi = coordinates.name match {
    case "mock" => entity.EntityApi(new URL("http://localhost:9999"))
    case _ => throw new Exception ("Entity not found")
  }

  override def getEntityTableName(coordinates: CatalogCoordinates): String = MetastoreCatalogTableNameBuilder.getTableName(coordinates);
}
