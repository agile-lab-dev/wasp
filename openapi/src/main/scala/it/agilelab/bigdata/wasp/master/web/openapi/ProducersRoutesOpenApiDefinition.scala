package it.agilelab.bigdata.wasp.master.web.openapi

import io.swagger.v3.oas.models.media.{Content, MediaType}
import io.swagger.v3.oas.models.parameters.{Parameter, RequestBody}
import io.swagger.v3.oas.models.responses.{ApiResponse, ApiResponses}
import io.swagger.v3.oas.models.{Operation, PathItem}
import it.agilelab.bigdata.wasp.models.ProducerModel

trait ProducersRoutesOpenApiDefinition
    extends ProducerOpenApiComponentSupport
    with AngularResponseOpenApiComponentSupport {

  def producersRoutes(ctx: Context): Map[String, PathItem] = {
    Map(
      "/producers" -> getInsertUpdate(ctx),
      "/producers/{producername}" -> delete(ctx),
      "/producers/{producername}/stop" -> stop(ctx),
      "/producers/{producername}/start" -> start(ctx)
    )
  }


  private def delete(ctx: Context): PathItem =
    new PathItem()
      .delete(
        new Operation().addTagsItem("producers")
          .operationId("delete-producer").description("Deletes a producer")
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("producername")
              .description("The name of the producer to delete")
              .schema(stringOpenApi.schema(ctx))
          )
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("Outcome of the operation")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[String]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )

  private def pretty(ctx: Context) = {
    new Parameter()
      .name("pretty")
      .in("query")
      .required(false)
      .schema(booleanOpenApi.schema(ctx))
  }

  private def getInsertUpdate(ctx: Context) = {
    new PathItem()
      .get(
        new Operation().addTagsItem("producers")
          .operationId("get-producer")
          .description("Retrieves all producers")
          .addParametersItem(pretty(ctx))
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("All producers")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[
                              Seq[ProducerModel]
                            ]].schema(ctx)
                          )
                      )
                  )
              )
          )
      )
      .post(
        new Operation().addTagsItem("producers")
          .operationId("insert-producer")
          .description("Inserts a producer")
          .addParametersItem(pretty(ctx))
          .requestBody(
            new RequestBody().content(
              new Content().addMediaType(
                "text/json",
                new MediaType()
                  .schema(ToOpenApiSchema[ProducerModel].schema(ctx))
              )
            )
          )
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("Result of insert")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[String]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )
      .put(
        new Operation().addTagsItem("producers")
          .description("Updates a new producer")
          .operationId("update-producer")
          .addParametersItem(pretty(ctx))
          .requestBody(
            new RequestBody().content(
              new Content().addMediaType(
                "text/json",
                new MediaType()
                  .schema(ToOpenApiSchema[ProducerModel].schema(ctx))
              )
            )
          )
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("Result of insert")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[String]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }

  private def start(ctx: Context) = {
    new PathItem()
      .post(
        new Operation().addTagsItem("producers")
          .operationId("start-producer")
          .description("Start a producer")
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("producername")
              .description("The name of the producer to start")
              .schema(stringOpenApi.schema(ctx))
          )
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("Outcome of the producer start operation")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[String]].schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }

  private def stop(ctx: Context) = {
    new PathItem()
      .post(
        new Operation().addTagsItem("producers")
          .operationId("stop-producer")
          .description("Stop a producerj")
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("producername")
              .description("The name of the producer to stop")
              .schema(stringOpenApi.schema(ctx))
          )
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("Outcome of the producer stop operation")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[String]].schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }


}
