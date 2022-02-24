package it.agilelab.bigdata.wasp.master.web.openapi

import io.swagger.v3.oas.models.media.Schema

import scala.reflect.ClassTag

trait OpenApiSchemaSupport {

  trait OpenApiRefOps{
    def toRef(ctx:Context): Schema[_]
  }

  implicit def toRefOps(schema:Schema[_]) : OpenApiRefOps = new OpenApiRefOps {
    override def toRef(ctx: Context): Schema[_] =  {
      val ref = s"#/components/schemas/${schema.getName}"
      ctx.register(ref, schema)
      new Schema().$ref(ref).nullable(schema.getNullable)
    }
  }

  def shouldBecomeARef[T: ClassTag](ctx: Context, x: Schema[_]) = {
    val named = x.getName != null
    if (named) {

      x.toRef(ctx)
    } else {
      x
    }
  }
}
