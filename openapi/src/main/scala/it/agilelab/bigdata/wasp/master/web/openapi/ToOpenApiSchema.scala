package it.agilelab.bigdata.wasp.master.web.openapi

import java.lang.reflect.Modifier
import java.lang.reflect.Modifier.TRANSIENT

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import io.swagger.v3.core.util.Yaml
import io.swagger.v3.oas.models.info.Info
import io.swagger.v3.oas.models.{Components, OpenAPI, Paths}
import io.swagger.v3.oas.models.media.{IntegerSchema, ObjectSchema, Schema, StringSchema}
import spray.json.ProductFormats

import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.util.control.NonFatal


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


