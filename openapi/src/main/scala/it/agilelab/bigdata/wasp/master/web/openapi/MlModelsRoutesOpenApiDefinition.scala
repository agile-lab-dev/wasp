package it.agilelab.bigdata.wasp.master.web.openapi

import io.swagger.v3.oas.models.media.{Content, MediaType}
import io.swagger.v3.oas.models.parameters.{Parameter, RequestBody}
import io.swagger.v3.oas.models.responses.{ApiResponse, ApiResponses}
import io.swagger.v3.oas.models.{Operation, PathItem}
import it.agilelab.bigdata.wasp.core.models.MlModelOnlyInfo

trait MlModelsRoutesOpenApiDefinition
    extends MlModelOnlyInfoComponentSupport
    with AngularResponseOpenApiComponentSupport {

  def mlmodelsRoutes(ctx: Context): Map[String, PathItem] = {
    Map(
      "/mlmodels" -> getInsertUpdate(ctx),
      "/mlmodels/{mlmodelname}/{mlmodelversion}" -> listInstance(ctx)
    )
  }

  private def listInstance(ctx: Context) = {
    new PathItem()
      .get(
        new Operation()
          .addTagsItem("mlmodels")
          .operationId("get-mlmodel")
              .description("Retrieves data on a specific Machine Learning model")
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("mlmodelname")
              .description("The name of the ml model to retrieve")
              .schema(stringOpenApi.schema(ctx))
          )
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("mlmodelversion")
              .description("The name of the mlmodel to retrieve")
              .schema(stringOpenApi.schema(ctx))
          )
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("The instance of the MlModel")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[MlModelOnlyInfo]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )
      .delete(
        new Operation()
          .addTagsItem("mlmodels")
        .operationId("delete-mlmodel")
          .description("Delete a Machine learning model")
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("mlmodelname")
              .description("The name of the ml model to retrieve")
              .schema(stringOpenApi.schema(ctx))
          )
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("mlmodelversion")
              .description("The name of the mlmodel to retrieve")
              .schema(stringOpenApi.schema(ctx))
          )
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("The instance of the MlModel")
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

  private def getInsertUpdate(ctx: Context) = {
    new PathItem()
      .get(
        new Operation()
          .operationId("list-mlmodel")
          .description("Retrieve all machine learning models info")
          .addTagsItem("mlmodels")
          .addParametersItem(pretty(ctx))
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("All mlmodels")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[
                              Seq[MlModelOnlyInfo]
                            ]].schema(ctx)
                          )
                      )
                  )
              )
          )
      )
      .post(
        new Operation()
          .addTagsItem("mlmodels")
          .operationId("insert-mlmodel")
          .description("Inserts a new MlModel")
          .addParametersItem(pretty(ctx))
          .requestBody(
            new RequestBody().content(
              new Content().addMediaType(
                "text/json",
                new MediaType()
                  .schema(ToOpenApiSchema[MlModelOnlyInfo].schema(ctx))
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
          .addTagsItem("mlmodels")
          .operationId("update-mlmodel")
          .description("Updates a machine learning model")
          .addParametersItem(pretty(ctx))
          .requestBody(
            new RequestBody().content(
              new Content().addMediaType(
                "text/json",
                new MediaType()
                  .schema(ToOpenApiSchema[MlModelOnlyInfo].schema(ctx))
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

}
