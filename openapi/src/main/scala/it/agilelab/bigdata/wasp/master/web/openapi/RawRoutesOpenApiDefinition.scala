package it.agilelab.bigdata.wasp.master.web.openapi

import io.swagger.v3.oas.models.media.{Content, MediaType}
import io.swagger.v3.oas.models.parameters.Parameter
import io.swagger.v3.oas.models.responses.{ApiResponse, ApiResponses}
import io.swagger.v3.oas.models.{Operation, PathItem}
import it.agilelab.bigdata.wasp.core.models.{RawModel, RawOptions}

trait RawModelOpenApiDefinition extends ProductOpenApi with LangOpenApi with CollectionsOpenApi {

  implicit lazy val rawModelOpenApiDefinition: ToOpenApiSchema[RawModel]     = product5(RawModel.apply)
  implicit lazy val rawOptionsOpenApiDefinition: ToOpenApiSchema[RawOptions] = product4(RawOptions.apply)

}

trait RawRoutesOpenApiDefinition extends RawModelOpenApiDefinition with AngularResponseOpenApiComponentSupport {

  def rawRoutes(ctx: Context): Map[String, PathItem] = {
    Map(
      "/raw"             -> get(ctx),
      "/raw/{modelname}" -> getInstance(ctx)
    )
  }

  private def getInstance(ctx: Context): PathItem =
    new PathItem()
      .get(
        new Operation()
          .addTagsItem("raw")
          .operationId("get-raw")
          .description("Retrieves the model used to write or read from Raw Stores")
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("modelname")
              .description("The name of the Raw model to retrieve")
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
                            ToOpenApiSchema[AngularResponse[RawModel]]
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

  private def get(ctx: Context) = {
    new PathItem()
      .get(
        new Operation()
          .operationId("list-raw")
          .description("Retrieves all models used to write or read from Raw Stores")
          .addTagsItem("documents")
          .addParametersItem(pretty(ctx))
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("All key value models")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[Seq[RawModel]]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }

}
