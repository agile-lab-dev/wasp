package it.agilelab.bigdata.wasp.master.web.openapi

import io.swagger.v3.oas.models.media.{
  ArraySchema,
  MapSchema,
  ObjectSchema,
  Schema
}

trait CollectionsOpenApi extends OpenApiSchemaSupport {
  implicit def optionOpenApi[T: ToOpenApiSchema]: ToOpenApiSchema[Option[T]] =
    new ToOpenApiSchema[Option[T]] {
      override def schema(ctx: Context): Schema[_] = {
        Schemas.copy(ToOpenApiSchema[T].schema(ctx)).nullable(true)
      }
    }

  implicit def listOpenApi[T: ToOpenApiSchema]: ToOpenApiSchema[List[T]] =
    new ToOpenApiSchema[List[T]] {
      override def schema(ctx: Context): Schema[_] = {
        val innerType = ToOpenApiSchema[T].schema(ctx)
        new ArraySchema().items(shouldBecomeARef(ctx, innerType))
      }
    }

  implicit def seqOpenApi[T: ToOpenApiSchema]: ToOpenApiSchema[Seq[T]] =
    new ToOpenApiSchema[Seq[T]] {
      override def schema(ctx: Context): Schema[_] = {
        val innerType = ToOpenApiSchema[T].schema(ctx)
        new ArraySchema().items(shouldBecomeARef(ctx, innerType))
      }
    }
  implicit def arrayOpenApi[T: ToOpenApiSchema]: ToOpenApiSchema[Array[T]] =
    new ToOpenApiSchema[Array[T]] {
      override def schema(ctx: Context): Schema[_] = {
        val innerType = ToOpenApiSchema[T].schema(ctx)
        new ArraySchema().items(shouldBecomeARef(ctx, innerType))
      }
    }

  implicit def mapOpenApy[T: ToOpenApiSchema]: ToOpenApiSchema[Map[String, T]] =
    new ToOpenApiSchema[Map[String, T]] {
      override def schema(ctx: Context): Schema[_] = {
        new ObjectSchema().additionalProperties(shouldBecomeARef(ctx,ToOpenApiSchema[T].schema(ctx)))
      }
    }

}
