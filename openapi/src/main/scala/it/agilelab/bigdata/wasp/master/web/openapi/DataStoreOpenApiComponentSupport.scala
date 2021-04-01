package it.agilelab.bigdata.wasp.master.web.openapi

import io.swagger.v3.oas.models.media.{ObjectSchema, Schema, StringSchema}
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct

trait DataStoreOpenApiComponentSupport extends OpenApiSchemaSupport with LangOpenApi {
  implicit lazy val dataStoreProductOpenApi: ToOpenApiSchema[DatastoreProduct] = stringOpenApi.substituteOf[DatastoreProduct]
}
