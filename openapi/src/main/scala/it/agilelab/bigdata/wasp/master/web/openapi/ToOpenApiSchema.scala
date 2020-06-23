package it.agilelab.bigdata.wasp.master.web.openapi

import io.swagger.v3.oas.models.media.Schema


trait Context {

  def register(ref: String, schema: Schema[_]) : Schema[_] = ???

}

trait ToOpenApiSchema[T] {
  self =>

  def schema(ctx : Context): Schema[_]

  def substituteOf[A]: ToOpenApiSchema[A] = mapSchema[A]((_, a) => a)

  def mapSchema[A](f: (Context, Schema[_]) => Schema[_]): ToOpenApiSchema[A] =
    new ToOpenApiSchema[A] {
      override def schema(ctx: Context): Schema[_] = {
        f(ctx, Schemas.copy(self.schema(ctx)))
      }
    }
}

object ToOpenApiSchema {
  def apply[T: ToOpenApiSchema] = implicitly[ToOpenApiSchema[T]]
}


