package it.agilelab.bigdata.wasp.master.web.openapi

import io.swagger.v3.core.util.Yaml
import io.swagger.v3.oas.models.media.Schema
import io.swagger.v3.oas.models.{Components, OpenAPI}

object OpenApiRenderer {

  def render(generator : Context => OpenAPI) : String = {

    val set = new java.util.HashMap[String, Schema[_]]()

    val context = new Context {
      override def register(ref: String, schema: Schema[_]): Schema[_] = {
        set.put(ref, schema)
      }
    }

    val definition = generator(context)

    val components = new Components()

    import scala.collection.JavaConverters._
    set.asScala.foreach{
      case (k, v) => components.addSchemas(v.getName, v)
    }

    definition.setComponents(components)

    Yaml.pretty(definition)
  }
}
