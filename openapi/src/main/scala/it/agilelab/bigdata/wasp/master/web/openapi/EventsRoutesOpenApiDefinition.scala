package it.agilelab.bigdata.wasp.master.web.openapi

import java.time.Instant

import io.swagger.v3.oas.models.media.{Content, MediaType}
import io.swagger.v3.oas.models.parameters.Parameter
import io.swagger.v3.oas.models.responses.{ApiResponse, ApiResponses}
import io.swagger.v3.oas.models.{Operation, PathItem}
import it.agilelab.bigdata.wasp.models.{EventEntry, Events}

trait EventsOpenApiComponentSupport
    extends ProductOpenApi
    with LangOpenApi
    with CollectionsOpenApi {
  implicit lazy val eventsOpenApi: ToOpenApiSchema[Events] = product2(Events.apply)
  implicit lazy val eventEntryOpenApi: ToOpenApiSchema[EventEntry] =
    product8(EventEntry.apply)
}

trait EventsRoutesOpenApiDefinition
    extends EventsOpenApiComponentSupport
    with AngularResponseOpenApiComponentSupport {

  def eventsRoutes(ctx: Context): Map[String, PathItem] = {
    Map("/events" -> get(ctx))
  }

  private def get(ctx: Context) = {
    new PathItem()
      .get(
        new Operation()
          .operationId("events")
          .description("Retrieves events entries")
          .addTagsItem("events")
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
                  .description("All event entries matching query")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[Events]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }

}
