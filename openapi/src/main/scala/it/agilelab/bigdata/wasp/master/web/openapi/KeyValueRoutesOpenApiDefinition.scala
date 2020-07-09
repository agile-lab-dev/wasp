package it.agilelab.bigdata.wasp.master.web.openapi

import io.swagger.v3.oas.models.media.{Content, MediaType}
import io.swagger.v3.oas.models.parameters.Parameter
import io.swagger.v3.oas.models.responses.{ApiResponse, ApiResponses}
import io.swagger.v3.oas.models.{Operation, PathItem}
import it.agilelab.bigdata.wasp.models.{KeyValueModel, KeyValueOption}

trait KeyValueModelOpenApiDefinition extends ProductOpenApi with LangOpenApi with CollectionsOpenApi {
  implicit lazy val keyValueModelOpenApi: ToOpenApiSchema[KeyValueModel] =
    product6(KeyValueModel.apply)

  implicit lazy val keyValueOptionOpenApi: ToOpenApiSchema[KeyValueOption] =
    product2(KeyValueOption.apply)
}

trait KeyValueRoutesOpenApiDefinition
    extends KeyValueModelOpenApiDefinition
    with AngularResponseOpenApiComponentSupport {

  def keyValueRoutes(ctx: Context): Map[String, PathItem] = {
    Map(
      "/keyvalue"                -> get(ctx),
      "/keyvalue/{modelname}" -> getInstance(ctx)
    )
  }

  private def getInstance(ctx: Context): PathItem =
    new PathItem()
      .get(
        new Operation()
          .addTagsItem("keyvalue")
          .operationId("get-keyvalue")
          .description("Retrieves the model used to write or read from KeyValue Stores")
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("modelname")
              .description("The name of the KeyValue model to retrieve")
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
                            ToOpenApiSchema[AngularResponse[KeyValueModel]]
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
          .operationId("list-keyvalue")
          .description("Retrieves all models used to write or read from KeyValue Stores")
          .addTagsItem("keyvalue")
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
                            ToOpenApiSchema[AngularResponse[Seq[KeyValueModel]]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }

}
