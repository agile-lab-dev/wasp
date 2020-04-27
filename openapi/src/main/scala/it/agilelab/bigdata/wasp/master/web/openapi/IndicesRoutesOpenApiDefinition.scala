package it.agilelab.bigdata.wasp.master.web.openapi

import io.swagger.v3.oas.models.media.{Content, MediaType}
import io.swagger.v3.oas.models.parameters.{Parameter, RequestBody}
import io.swagger.v3.oas.models.responses.{ApiResponse, ApiResponses}
import io.swagger.v3.oas.models.{Operation, PathItem}
import it.agilelab.bigdata.wasp.core.models.{DocumentModel, IndexModel, ProducerModel}

trait IndicesModelOpenApiComponentSupport extends ProducerOpenApiComponentSupport with LangOpenApi with CollectionsOpenApi {

  implicit val indexModelOpenApi: ToOpenApiSchema[IndexModel] = product8(IndexModel)
}
trait IndicesRoutesOpenApiDefinition
    extends IndicesModelOpenApiComponentSupport
    with AngularResponseOpenApiComponentSupport {

  def indicesRoutes(ctx: Context): Map[String, PathItem] = {
    Map(
      "/indices" -> get(ctx),
      "/index" -> get(ctx),
      "/index/{indexname}" -> getInstance(ctx)
    )
  }

  private def getInstance(ctx: Context): PathItem =
    new PathItem()
      .get(
        new Operation()
          .operationId("GetIndex")
          .description("Retrieves all models used to read or write Indexed data stores")
          .addTagsItem("indices")
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("indexname")
              .description("The name of the index model to retrieve")
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
                            ToOpenApiSchema[AngularResponse[IndexModel]]
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
          .operationId("ListIndices")
          .description("Retrieve all models used to read or write indexed Data Stores")
          .addTagsItem("indices")
          .addParametersItem(pretty(ctx))
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("All indices model")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[Seq[IndexModel]]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }

}
