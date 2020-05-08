package it.agilelab.bigdata.wasp.master.web.openapi

import java.time.Instant

import io.swagger.v3.oas.models.media.{Content, MediaType}
import io.swagger.v3.oas.models.parameters.Parameter
import io.swagger.v3.oas.models.responses.{ApiResponse, ApiResponses}
import io.swagger.v3.oas.models.{Operation, PathItem}
import it.agilelab.bigdata.wasp.core.models.{LogEntry, Logs, MetricEntry, SourceEntry, TelemetryPoint, TelemetrySeries}

trait TelemetryOpenApiComponentSupport extends ProductOpenApi with LangOpenApi with CollectionsOpenApi {
  implicit lazy val sourceEntryOpenApi: ToOpenApiSchema[SourceEntry] = product1(SourceEntry.apply)
  implicit lazy val metricEntryOpenApi: ToOpenApiSchema[MetricEntry] = product2(MetricEntry.apply)
  implicit lazy val seriesOpenApi: ToOpenApiSchema[TelemetrySeries]  = product3(TelemetrySeries.apply)
  implicit lazy val telemetryPointOpenApi: ToOpenApiSchema[TelemetryPoint] =
    product2(TelemetryPoint.apply)
}

trait TelemetrRoutesOpenApiDefinition
    extends TelemetryOpenApiComponentSupport
    with AngularResponseOpenApiComponentSupport {

  def telemetryRoutes(ctx: Context): Map[String, PathItem] = {
    Map("/telemetry/sources" -> sources(ctx), "/telemetry/metrics" -> metrics(ctx), "/telemetry/series" -> series(ctx))
  }

  private def sources(ctx: Context) = {
    new PathItem()
      .get(
        new Operation()
          .operationId("list-telemetry-sources")
          .description("List top telemetry sources matching search")
          .addTagsItem("telemetry")
          .addParametersItem(
            new Parameter()
              .name("search")
              .in("query")
              .required(true)
              .schema(stringOpenApi.schema(ctx))
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
                  .description("Top sources matching query")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[SourceEntry]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }

  private def metrics(ctx: Context) = {
    new PathItem()
      .get(
        new Operation()
          .operationId("list-telemetry-metrics")
          .description("List top telemetry metrics for source matching search")
          .addTagsItem("telemetry")
          .addParametersItem(
            new Parameter()
              .name("search")
              .in("query")
              .required(true)
              .schema(stringOpenApi.schema(ctx))
          )
          .addParametersItem(
            new Parameter()
              .name("source")
              .in("query")
              .required(true)
              .schema(stringOpenApi.schema(ctx))
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
                  .description("Top sources matching query")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[MetricEntry]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }

  private def series(ctx: Context) = {
    new PathItem()
      .get(
        new Operation()
          .operationId("get-telemetry-series")
          .description("Retrieves series data pre aggregated by the server for display")
          .addTagsItem("telemetry")
          .addParametersItem(
            new Parameter()
              .name("metric")
              .in("query")
              .required(true)
              .schema(stringOpenApi.schema(ctx))
          )
          .addParametersItem(
            new Parameter()
              .name("source")
              .in("query")
              .required(true)
              .schema(stringOpenApi.schema(ctx))
          )
          .addParametersItem(
            new Parameter()
              .name("size")
              .in("query")
              .description(
                "The number of buckets to aggregate data in, the start and end timestamp" +
                  " will be divided in [size] number buckets and data will be averaged inside buckets"
              )
              .required(true)
              .schema(integerOpenApi.schema(ctx))
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
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("Series data matching query")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[TelemetrySeries]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }

}
