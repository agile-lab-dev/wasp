package it.agilelab.bigdata.wasp.master.web.openapi



import io.swagger.v3.oas.models.media.{Content, MediaType}
import io.swagger.v3.oas.models.parameters.{Parameter, RequestBody}
import io.swagger.v3.oas.models.responses.{ApiResponse, ApiResponses}
import io.swagger.v3.oas.models.{Operation, PathItem}
import it.agilelab.bigdata.wasp.compiler.utils.{CompletionModel, ErrorModel}
import it.agilelab.bigdata.wasp.core.models.FreeCodeModel




trait FreeCodeRoutesOpenApiDefinition extends
  FreeCodeModelOpenApiSupport with
  AngularResponseOpenApiComponentSupport {

  def freeCodeRoutes(ctx: Context): Map[String, PathItem] = {
    Map(
      "/freeCode"                -> get(ctx),
      "/freeCode/{modelname}" -> getInstance(ctx),
      "/freeCode/complete/{position}" -> getInstanceComplete(ctx)
    )
  }

  private def pretty(ctx: Context) = {
    new Parameter()
      .name("pretty")
      .in("query")
      .required(false)
      .schema(booleanOpenApi.schema(ctx))
  }


  private def getInstance(ctx: Context): PathItem =
    new PathItem()
      .get(
        new Operation()
          .addTagsItem("freeCode")
          .operationId("get-freeCode")
          .description("Retrieves the models used to create a free code strategy")
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("modelname")
              .description("The name of the freeCode model to retrieve")
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
                            ToOpenApiSchema[AngularResponse[FreeCodeModel]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )

  private def getInstanceComplete(ctx: Context): PathItem =
    new PathItem()
      .post(
        new Operation()
          .operationId("complete-freeCode")
          .description("complete the code of a freeCode model")
          .addTagsItem("freeCode")
          .description("complete the code of a freeCode model")
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("position")
              .description("The position on the code to complete")
              .schema(integerOpenApi.schema(ctx))
          )
          .requestBody(
            new RequestBody().content(
              new Content().addMediaType(
                "application/json",
                new MediaType()
                  .schema(ToOpenApiSchema[FreeCodeModel].schema(ctx))
              )
            )
          )
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("complete the code of a freeCode model")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[Seq[CompletionModel]]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )



  private def get(ctx: Context) = {
    new PathItem()
      .get(
        new Operation()
          .operationId("list-freeCode")
          .description("Retrieves the models used to create a free code strategy")
          .addTagsItem("freeCode")
          .addParametersItem(pretty(ctx))
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("All free code strategy")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[Seq[FreeCodeModel]]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )
      .post(
        new Operation()
          .operationId("insert-freeCode")
          .description("Inserts a freeCode model")
          .addTagsItem("freeCode")
          .description("Inserts a freeCode model")
          .addParametersItem(pretty(ctx))
          .requestBody(
            new RequestBody().content(
              new Content().addMediaType(
                "application/json",
                new MediaType()
                  .schema(ToOpenApiSchema[FreeCodeModel].schema(ctx))
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
                            ToOpenApiSchema[InsertResponse]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }


}
