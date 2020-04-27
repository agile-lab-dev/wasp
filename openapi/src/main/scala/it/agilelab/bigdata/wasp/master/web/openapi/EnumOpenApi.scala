package it.agilelab.bigdata.wasp.master.web.openapi

import java.net.URLEncoder

import io.swagger.v3.oas.models.media.{Schema, StringSchema}

import scala.reflect.ClassTag

trait EnumOpenApi extends OpenApiSchemaSupport {
  def enumOpenApi[T <: Enumeration : ClassTag](enum: T): ToOpenApiSchema[T#Value] = new ToOpenApiSchema[T#Value] {
    override def schema(ctx: Context): Schema[_] = {
      val enumSchema = new StringSchema()
      enum.values.foreach(x => enumSchema.addEnumItem(x.toString))

      shouldBecomeARef(ctx, enumSchema.name(implicitly[ClassTag[T]].runtimeClass.getSimpleName.replace("$","")))
    }
  }
}
