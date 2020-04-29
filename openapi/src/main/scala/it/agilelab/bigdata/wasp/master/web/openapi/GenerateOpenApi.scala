package it.agilelab.bigdata.wasp.master.web.openapi
import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.info.Info
import io.swagger.v3.oas.models.tags.Tag
import it.agilelab.bigdata.wasp.core.build.BuildInfo

object GenerateOpenApi
    extends BatchJobRoutesOpenApiDefinition
    with PipegraphRoutesOpenApiDefinition
    with ProducersRoutesOpenApiDefinition
    with DocumentRoutesOpenApiDefinition
    with IndicesRoutesOpenApiDefinition
    with TopicRoutesOpenApiDefinition
    with MlModelsRoutesOpenApiDefinition
    with ConfigRoutesOpenApiDefinition
    with LogsRoutesOpenApiDefinition {

  def main(args: Array[String]): Unit = {

    val generate = (ctx: Context) => {

      val routes = batchJobRoutes(ctx) ++ pipegraphRoutes(ctx) ++
        producersRoutes(ctx) ++ indicesRoutes(ctx) ++
        topicRoute(ctx) ++ documentsRoutes(ctx) ++
        mlmodelsRoutes(ctx) ++ configRoute(ctx) ++
        logsRoutes(ctx)

      val openapi = new OpenAPI()
        .info(new Info().title("wasp-api").version(BuildInfo.version))
        .addTagsItem(
          new Tag()
            .name("batchjobs")
            .description("operation related to batchjobs management")
        )
        .addTagsItem(
          new Tag()
            .name("pipegraphs")
            .description("operation related to pipegraphs management")
        )
        .addTagsItem(
          new Tag()
            .name("producers")
            .description("operation related to producers management")
        )
        .addTagsItem(
          new Tag()
            .name("documents")
            .description("operation related to documents management")
        )
        .addTagsItem(
          new Tag()
            .name("topics")
            .description("operation related to topics management")
        )
        .addTagsItem(
          new Tag()
            .name("indices")
            .description("operation related to indices management")
        )
        .addTagsItem(
          new Tag()
            .name("mlmodels")
            .description(
              "operation related to machine learning models management"
            )
        )
        .addTagsItem(
          new Tag()
            .name("configuration")
            .description("operation related to configurations management")
        )
        .addTagsItem(
          new Tag()
            .name("logs")
            .description("operation related to logs inspection")
        )

      routes.foreach {
        case (key, value) => openapi.path(key, value)
      }

      openapi
    }

    println(OpenApiRenderer.render(generate))
  }
}
