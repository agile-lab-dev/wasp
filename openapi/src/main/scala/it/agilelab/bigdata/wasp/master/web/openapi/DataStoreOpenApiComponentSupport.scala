package it.agilelab.bigdata.wasp.master.web.openapi

import it.agilelab.bigdata.wasp.datastores.DatastoreProduct

trait DataStoreOpenApiComponentSupport extends OpenApiSchemaSupport with LangOpenApi {
  implicit lazy val dataStoreProductOpenApi: ToOpenApiSchema[DatastoreProduct] = stringOpenApi.substituteOf[DatastoreProduct]
}
