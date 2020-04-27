package it.agilelab.bigdata.wasp.master.web.openapi

import io.swagger.v3.oas.models.media.{Content, MediaType}
import io.swagger.v3.oas.models.parameters.Parameter
import io.swagger.v3.oas.models.responses.{ApiResponse, ApiResponses}
import io.swagger.v3.oas.models.{Operation, PathItem}
import it.agilelab.bigdata.wasp.core.models.TopicModel

trait TopicRoutesOpenApiDefinition
    extends TopicModelOpenApiComponentSupport
    with AngularResponseOpenApiComponentSupport {

  def topicRoute(ctx: Context): Map[String, PathItem] = {
    Map("/topics" -> get(ctx), "/topics/{topicname}" -> getInstance(ctx))
  }

  private def getInstance(ctx: Context): PathItem =
    new PathItem()
      .get(
        new Operation()
          .operationId("GetTopic")
          .description(
            "Retrieves the model used to write or read from message queues"
          )
          .addTagsItem("topics")
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("topicname")
              .description("The name of the topic to retrieve")
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
                            ToOpenApiSchema[AngularResponse[TopicModel]]
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
          .operationId("GetTopics")
          .description(
            "Lists all models used to write or read from message queues"
          )
          .addTagsItem("topics")
          .addParametersItem(pretty(ctx))
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("All topics")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[Seq[TopicModel]]]
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
