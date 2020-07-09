package it.agilelab.bigdata.wasp.master.web.openapi

import io.swagger.v3.oas.models.media.{ObjectSchema, Schema, StringSchema}
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct

trait DataStoreOpenApiComponentSupport extends OpenApiSchemaSupport {
  implicit lazy val dataStoreProductOpenApi: ToOpenApiSchema[DatastoreProduct] =
    new ToOpenApiSchema[DatastoreProduct] {
      override def schema(ctx : Context): Schema[_] = {

        val category = new StringSchema()
        val product = new StringSchema()

        DatastoreProduct.productsLookupMap.values.foreach { p =>
          category.addEnumItem(p.categoryName)
          product.addEnumItem(p.productName.getOrElse(""))
        }



        new ObjectSchema()
          .addProperties("category", shouldBecomeARef(ctx, category.name("DataStoreCategoryEnum")))
          .addProperties("product", shouldBecomeARef(ctx, product.name("DataStoreProductEnum")))
          .name("DatastoreProduct")
      }
    }
}
