package it.agilelab.bigdata.wasp.master.web.openapi

import io.swagger.v3.oas.models.media.{Content, MediaType}
import io.swagger.v3.oas.models.parameters.{Parameter, RequestBody}
import io.swagger.v3.oas.models.responses.{ApiResponse, ApiResponses}
import io.swagger.v3.oas.models.{Operation, PathItem}
import it.agilelab.bigdata.wasp.core.models.editor.{CodeResponse, NifiStatelessInstanceModel, PipegraphDTO}

trait EditorRoutesOpenApiDefinition extends EditorOpenApiComponentSupport with AngularResponseOpenApiComponentSupport {

  //TODO:       "/editor/pipegraph" -> storePipegraphDTO(ctx) returns the pipegraphModel?

  def editorRoutes(ctx: Context): Map[String, PathItem] = {
    Map(
      "/editor/nifi/{processGroupName}"        -> newNifiEditor(ctx),
      "/editor/nifi/registry/{processGroupId}" -> commitEditorProcessGroup(ctx),
      "/editor/pipegraph" -> storePipegraphDTO(ctx)
    )
  }

  def newNifiEditor(ctx: Context): PathItem = {
    new PathItem()
      .post(
        new Operation()
          .addTagsItem("editor")
          .operationId("new-editor")
          .description(
            "Create a new processGroup on a stateless nifi instance with name processGroupName," +
              " returns a processgroupId and the instance url."
          )
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("Path")
              .name("processGroupName")
              .description("The name of the new processGroup")
              .schema(stringOpenApi.schema(ctx))
          )
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("The new editor instance")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[NifiStatelessInstanceModel]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }

  def commitEditorProcessGroup(ctx: Context): PathItem = {
    new PathItem()
      .put(
        new Operation()
          .addTagsItem("editor")
          .operationId("commit-processgroup")
          .description(
            "Confirm that a process group edited on a editor instance it's ready to be stored."
          )
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("Path")
              .name("processgroupid")
              .description("The process group id to commit")
              .schema(stringOpenApi.schema(ctx))
          )
          .responses(
            new ApiResponses().addApiResponse(
              "200",
              new ApiResponse()
                .description("Result of the commit")
                .content(
                  new Content()
                    .addMediaType(
                      "text/json",
                      new MediaType()
                        .schema(
                          ToOpenApiSchema[AngularResponse[CodeResponse]].schema(ctx)
                        )
                    )
                )
            )
          )
      )
  }

  def storePipegraphDTO(ctx: Context): PathItem = {
    new PathItem()
      .post(
        new Operation()
          .addTagsItem("editor")
          .operationId("post-pipegraph")
          .description(
            "Pipegraph generation from RawPipegraph"
          )
          .addParametersItem(pretty(ctx))
          .requestBody(
            new RequestBody().content(
              new Content().addMediaType(
                "text/json",
                new MediaType()
                  .schema(ToOpenApiSchema[PipegraphDTO].schema(ctx))
              )
            )
          )
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("Pipegraph generation result")
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

  private def pretty(ctx: Context) = {
    new Parameter()
      .name("pretty")
      .in("query")
      .required(false)
      .schema(booleanOpenApi.schema(ctx))
  }

}
