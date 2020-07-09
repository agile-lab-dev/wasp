package it.agilelab.bigdata.wasp.master.web.openapi

import java.time.Instant

import io.swagger.v3.oas.models.media.{Content, MediaType}
import io.swagger.v3.oas.models.parameters.Parameter
import io.swagger.v3.oas.models.responses.{ApiResponse, ApiResponses}
import io.swagger.v3.oas.models.{Operation, PathItem}
import it.agilelab.bigdata.wasp.models.{CountEntry, Counts}

trait StatsOpenApiComponentSupport extends ProductOpenApi with LangOpenApi with CollectionsOpenApi {
  implicit lazy val countEntryOpenApi: ToOpenApiSchema[CountEntry] = product2(CountEntry.apply)
  implicit lazy val countsOpenApi: ToOpenApiSchema[Counts] =
    product3(Counts.apply)
}

trait StatsRoutesOpenApiDefinition extends StatsOpenApiComponentSupport with AngularResponseOpenApiComponentSupport {

  def statsRoutes(ctx: Context): Map[String, PathItem] = {
    Map("/stats" -> get(ctx))
  }

  private def get(ctx: Context) = {
    new PathItem()
      .get(
        new Operation()
          .operationId("stats")
          .description("Retrieve stats about logs, events and metrics")
          .addTagsItem("stats")
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
              .name("size")
              .in("query")
              .required(true)
              .schema(ToOpenApiSchema[Int].schema(ctx))
          ).responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("stats matching query")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[Counts]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }

}
