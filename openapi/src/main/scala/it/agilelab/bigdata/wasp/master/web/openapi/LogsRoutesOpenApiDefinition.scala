package it.agilelab.bigdata.wasp.master.web.openapi

import java.time.Instant

import io.swagger.v3.oas.models.media.{Content, MediaType}
import io.swagger.v3.oas.models.parameters.Parameter
import io.swagger.v3.oas.models.responses.{ApiResponse, ApiResponses}
import io.swagger.v3.oas.models.{Operation, PathItem}
import it.agilelab.bigdata.wasp.models.{LogEntry, Logs}

trait LogsOpenApiComponentSupport
    extends ProducerOpenApiComponentSupport
    with LangOpenApi
    with CollectionsOpenApi {
  implicit lazy val logsOpenApi: ToOpenApiSchema[Logs] = product2(Logs.apply)
  implicit lazy val logsEntryOpenApi: ToOpenApiSchema[LogEntry] =
    product7(LogEntry.apply)
}

trait LogsRoutesOpenApiDefinition
    extends LogsOpenApiComponentSupport
    with AngularResponseOpenApiComponentSupport {

  def logsRoutes(ctx: Context): Map[String, PathItem] = {
    Map("/logs" -> get(ctx))
  }

  private def get(ctx: Context) = {
    new PathItem()
      .get(
        new Operation()
          .operationId("logs")
          .description("Retrieves logs entries")
          .addTagsItem("logs")
          .addParametersItem(
            new Parameter()
              .name("search")
              .in("query")
              .required(true)
              .schema(stringOpenApi.schema(ctx))
          )
          .addParametersItem(
            new Parameter()
              .name("startTimestamp")
              .in("query")
              .required(true)
              .schema(ToOpenApiSchema[Instant].schema(ctx))
          )
          .addParametersItem(
            new Parameter()
              .name("endTimestamp")
              .in("query")
              .required(true)
              .schema(ToOpenApiSchema[Instant].schema(ctx))
          )
          .addParametersItem(
            new Parameter()
              .name("page")
              .in("query")
              .required(true)
              .schema(integerOpenApi.schema(ctx))
          )
          .addParametersItem(
            new Parameter()
              .name("size")
              .in("query")
              .required(true)
              .schema(integerOpenApi.schema(ctx))
          )
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("All log entries matching query")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[Logs]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }

}
