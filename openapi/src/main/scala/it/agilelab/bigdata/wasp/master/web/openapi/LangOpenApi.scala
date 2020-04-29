package it.agilelab.bigdata.wasp.master.web.openapi

import java.time.Instant

import io.swagger.v3.oas.models.media.{
  BooleanSchema,
  DateTimeSchema,
  IntegerSchema,
  ObjectSchema,
  Schema,
  StringSchema
}

trait LangOpenApi {

  implicit lazy val stringOpenApi: ToOpenApiSchema[String] =
    new ToOpenApiSchema[String] {
      override def schema(ctx: Context): Schema[_] = new StringSchema
    }

  implicit lazy val longOpenApi: ToOpenApiSchema[Long] =
    new ToOpenApiSchema[Long] {
      override def schema(ctx: Context): Schema[_] =
        new IntegerSchema().format("int64")
    }

  implicit lazy val integerOpenApi: ToOpenApiSchema[Int] =
    new ToOpenApiSchema[Int] {
      override def schema(ctx: Context): Schema[_] =
        new IntegerSchema().format("int32")
    }

  implicit lazy val booleanOpenApi: ToOpenApiSchema[Boolean] =
    new ToOpenApiSchema[Boolean] {
      override def schema(ctx: Context): Schema[_] = new BooleanSchema()
    }

  def objectOpenApi[T]: ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
    override def schema(ctx: Context): Schema[_] = new ObjectSchema()
  }

  implicit lazy val instantOpenApi: ToOpenApiSchema[Instant] =
    new ToOpenApiSchema[Instant] {
      override def schema(ctx: Context): Schema[_] = {
        new DateTimeSchema()
      }
    }

}
