package it.agilelab.bigdata.wasp.master.web.openapi

import io.swagger.v3.oas.models.media.{Content, MediaType}
import io.swagger.v3.oas.models.parameters.{Parameter, RequestBody}
import io.swagger.v3.oas.models.responses.{ApiResponse, ApiResponses}
import io.swagger.v3.oas.models.{Operation, PathItem}
import it.agilelab.bigdata.wasp.models.{PipegraphInstanceModel, PipegraphModel}

trait PipegraphRoutesOpenApiDefinition
    extends PipegraphOpenApiComponentSupport
    with AngularResponseOpenApiComponentSupport {

  def pipegraphRoutes(ctx: Context): Map[String, PathItem] = {
    Map(
      "/pipegraphs" -> getInsertUpdate(ctx),
      "/pipegraphs/{pipegraphname}" -> delete(ctx),
      "/pipegraphs/{pipegraphname}/instances" -> listInstances(ctx),
      "/pipegraphs/{pipegraphname}/stop" -> stop(ctx),
      "/pipegraphs/{pipegraphname}/start" -> start(ctx),
      "/pipegraphs/{pipegraphname}/instances/{instance}" -> listInstance(ctx)
    )
  }
  private def listInstance(ctx: Context) = {
    new PathItem()
      .get(
        new Operation()
          .addTagsItem("pipegraphs")
          .operationId("get-pipegraph-instance")
          .description("Retrieves a pipegraph instance")
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("pipegraphname")
              .description("The name of the pipegraph to list instance for")
              .schema(stringOpenApi.schema(ctx))
          )
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("instance")
              .description("The name of the pipegraph instance to list")
              .schema(stringOpenApi.schema(ctx))
          )
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("The instance of the pipegraph")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[
                              PipegraphInstanceModel
                            ]].schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }

  private def pretty(ctx: Context) = {
    new Parameter()
      .name("pretty")
      .in("query")
      .required(false)
      .schema(booleanOpenApi.schema(ctx))
  }

  private def delete(ctx: Context): PathItem =
    new PathItem()
      .delete(
        new Operation()
          .addTagsItem("pipegraphs")
          .operationId("delete-pipegraph")
          .description("Deletes a pipegraph")
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("pipegraphname")
              .description("The name of the pipegraph to delete")
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

  private def getInsertUpdate(ctx: Context) = {
    new PathItem()
      .get(
        new Operation()
          .operationId("list-pipegraph")
          .description("Lists all pipegraphs")
          .addTagsItem("pipegraphs")
          .addParametersItem(pretty(ctx))
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("All pipegraphs")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[
                              Seq[PipegraphModel]
                            ]].schema(ctx)
                          )
                      )
                  )
              )
          )
      )
      .post(
        new Operation()
          .operationId("insert-pipegraph")
          .description("Insert a pipegraph")
          .addTagsItem("pipegraphs")
          .description("Inserts a pipegraph")
          .addParametersItem(pretty(ctx))
          .requestBody(
            new RequestBody().content(
              new Content().addMediaType(
                "text/json",
                new MediaType()
                  .schema(ToOpenApiSchema[PipegraphModel].schema(ctx))
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
        new Operation()
          .addTagsItem("pipegraphs")
          .operationId("update-pipegraph")
          .description("updateds a pipegraph")
          .addParametersItem(pretty(ctx))
          .requestBody(
            new RequestBody().content(
              new Content().addMediaType(
                "text/json",
                new MediaType()
                  .schema(ToOpenApiSchema[PipegraphModel].schema(ctx))
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
        new Operation()
          .operationId("start-pipegraph")
          .description("Starts a new instance of pipegraph")
          .addTagsItem("pipegraphs")
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("pipegraphname")
              .description("The name of the pipegraph to start")
              .schema(stringOpenApi.schema(ctx))
          )
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("Outcome of the pipegraph start operation")
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
        new Operation()
          .addTagsItem("pipegraphs")
          .operationId("stop-pipegraph")
          .description("Stops the running instance of a pipegrah")
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("pipegraphname")
              .description("The name of the pipegraph to stop")
              .schema(stringOpenApi.schema(ctx))
          )
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("Outcome of the pipegraph stop operation")
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

  private def listInstances(ctx: Context) = {
    new PathItem()
      .post(
        new Operation()
          .operationId("list-pipegraph-instance")
          .description("List all instances of a pipegraph")
          .addTagsItem("pipegraphs")
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("pipegraphname")
              .description(
                "The name of the pipegraph whose instances should be listed"
              )
              .schema(stringOpenApi.schema(ctx))
          )
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("All the instances for the requested pipegraph")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[
                              Seq[PipegraphInstanceModel]
                            ]].schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }

}
