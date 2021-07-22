package it.agilelab.bigdata.wasp.master.web.openapi

import io.swagger.v3.oas.models.media.{Content, MediaType}
import io.swagger.v3.oas.models.parameters.Parameter
import io.swagger.v3.oas.models.responses.{ApiResponse, ApiResponses}
import io.swagger.v3.oas.models.{Operation, PathItem}
import it.agilelab.bigdata.wasp.datastores.GenericProduct
import it.agilelab.bigdata.wasp.models.{GenericModel, GenericOptions}

trait GenericModelOpenApiDefinition extends ProductOpenApi with BsonDocumentOpenApiDefinition with LangOpenApi with CollectionsOpenApi {

  implicit lazy val genericModelOpenApiDefinition: ToOpenApiSchema[GenericModel]     = product4(GenericModel.apply)
  implicit lazy val genericProductOpenApiDefinition: ToOpenApiSchema[GenericProduct]     = product2(GenericProduct.apply)
  implicit lazy val genericOptionsOpenApiDefinition: ToOpenApiSchema[GenericOptions] = product1(GenericOptions.apply)

}

trait GenericRoutesOpenApiDefinition extends GenericModelOpenApiDefinition with AngularResponseOpenApiComponentSupport {

  def genericRoutes(ctx: Context): Map[String, PathItem] = {
    Map(
      "/generic"             -> get(ctx),
      "/generic/{modelname}" -> getInstance(ctx)
    )
  }

  private def getInstance(ctx: Context): PathItem =
    new PathItem()
      .get(
        new Operation()
          .addTagsItem("generic")
          .operationId("get-generic")
          .description("Retrieves the model used to write or read from Generic Stores")
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("modelname")
              .description("The name of the Generic model to retrieve")
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
                            ToOpenApiSchema[AngularResponse[GenericModel]]
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
          .operationId("list-generic")
          .description("Retrieves all models used to write or read from Generic Stores")
          .addTagsItem("generic")
          .addParametersItem(pretty(ctx))
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("All Generic models")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[Seq[GenericModel]]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }

}
