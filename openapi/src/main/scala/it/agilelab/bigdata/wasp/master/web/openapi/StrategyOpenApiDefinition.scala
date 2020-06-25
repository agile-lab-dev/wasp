package it.agilelab.bigdata.wasp.master.web.openapi

import io.swagger.v3.oas.models.{Operation, PathItem}
import io.swagger.v3.oas.models.media.{Content, MediaType}
import io.swagger.v3.oas.models.parameters.Parameter
import io.swagger.v3.oas.models.responses.{ApiResponse, ApiResponses}

trait StrategyOpenApiDefinition extends LangOpenApi with CollectionsOpenApi with AngularResponseOpenApiComponentSupport{


  def strategiesRoutes(ctx: Context): Map[String, PathItem] = {
    Map(
      "/strategy"                -> get(ctx)
    )
  }

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
          .operationId("list-strategies")
          .description("Retrieves the possible strategies")
          .addTagsItem("strategy")
          .addParametersItem(pretty(ctx))
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("All strategies")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[Seq[String]]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }


}
