package it.agilelab.bigdata.wasp.master.web.openapi

import io.swagger.v3.oas.models.media.{Content, MediaType}
import io.swagger.v3.oas.models.parameters.{Parameter, RequestBody}
import io.swagger.v3.oas.models.responses.{ApiResponse, ApiResponses}
import io.swagger.v3.oas.models.{Operation, PathItem}
import it.agilelab.bigdata.wasp.core.models.{DocumentModel, ProducerModel}

trait DocumentModelOpenApiComponentSupport extends ProducerOpenApiComponentSupport with LangOpenApi with CollectionsOpenApi {

  implicit lazy val documentModelOpenApi: ToOpenApiSchema[DocumentModel] = product3(DocumentModel)
}
trait DocumentRoutesOpenApiDefinition
    extends DocumentModelOpenApiComponentSupport
    with AngularResponseOpenApiComponentSupport {

  def documentsRoutes(ctx: Context): Map[String, PathItem] = {
    Map(
      "/document" -> get(ctx),
      "/document/{documentname}" -> getInstance(ctx)
    )
  }

  private def getInstance(ctx: Context): PathItem =
    new PathItem()
      .get(
        new Operation()
          .addTagsItem("documents")
          .operationId("GetDocument")
          .description("Retrieves the model used to write or read from Document Stores")
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("documentname")
              .description("The name of the document model to retrieve")
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
                            ToOpenApiSchema[AngularResponse[DocumentModel]]
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
          .operationId("ListDocuments")
          .description("Retrieves all models used to write or read from Document Stores")
          .addTagsItem("documents")
          .addParametersItem(pretty(ctx))
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("All documents")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[Seq[DocumentModel]]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }

}
