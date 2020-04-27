package it.agilelab.bigdata.wasp.master.web.openapi

import io.swagger.v3.oas.models.media.{Content, MediaType}
import io.swagger.v3.oas.models.parameters.{Parameter, RequestBody}
import io.swagger.v3.oas.models.responses.{ApiResponse, ApiResponses}
import io.swagger.v3.oas.models.{Operation, PathItem}
import it.agilelab.bigdata.wasp.core.models.{
  BatchJobInstanceModel,
  BatchJobModel
}

trait BatchJobRoutesOpenApiDefinition
    extends BatchOpenApiComponentsSupport
    with AngularResponseOpenApiComponentSupport {

  def batchJobRoutes(ctx: Context): Map[String, PathItem] = {
    Map(
      "/batchjobs" -> getInsertUpdate(ctx),
      "/batchjobs/{batchjobname}" -> delete(ctx),
      "/batchjobs/{batchjobname}/start" -> start(ctx),
      "/batchjobs/{batchjobname}/instances" -> listInstances(ctx),
      "/batchjobs/{batchjobname}/instances/{instance}" -> listInstance(ctx)
    )
  }

  private def delete(ctx: Context): PathItem =
    new PathItem()
      .delete(
        new Operation()
          .addTagsItem("batchjobs")
          .operationId("DeleteBatchJob")
          .description("Deletes a Batch Job")
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("batchjobname")
              .description("The name of the batch job to delete")
              .schema(stringOpenApi.schema(ctx))
          )
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("Outcome of the batch job start operation")
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

  private def getInsertUpdate(ctx: Context) = {
    new PathItem()
      .get(
        new Operation()
          .operationId("GetBarchJobs")
          .description("Lists all barch jobs")
          .addTagsItem("batchjobs")
          .addParametersItem(pretty(ctx))
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("All batch jobs")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[Seq[BatchJobModel]]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )
      .post(
        new Operation()
          .operationId("InsertBatchJob")
          .description("Inserts a batch job")
          .addTagsItem("batchjobs")
          .description("Inserts a new batch job")
          .addParametersItem(pretty(ctx))
          .requestBody(
            new RequestBody().content(
              new Content().addMediaType(
                "text/json",
                new MediaType().schema(batchJobModelOpenApi.schema(ctx))
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
          .operationId("UpdateBatchJob")
          .description("Updates a batch job")
          .addTagsItem("batchjobs")
          .description("Inserts a new batch job")
          .addParametersItem(pretty(ctx))
          .requestBody(
            new RequestBody().content(
              new Content().addMediaType(
                "text/json",
                new MediaType().schema(batchJobModelOpenApi.schema(ctx))
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

  private def start(ctx: Context) = {
    new PathItem()
      .post(
        new Operation()
          .addTagsItem("batchjobs")
          .operationId("StartBatchJob")
          .description("Starts a batch job")
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("batchjobname")
              .description("The name of the batch job to start")
              .schema(stringOpenApi.schema(ctx))
          )
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("Outcome of the batch job start operation")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[
                              BatchJobStartResult
                            ]].schema(ctx)
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

  private def listInstances(ctx: Context) = {
    new PathItem()
      .get(
        new Operation()
          .operationId("GetBatchJobInstances")
          .description("Lists all instances of a batch job")
          .addTagsItem("batchjobs")
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("batchjobname")
              .description("The name of the batch job to start")
              .schema(stringOpenApi.schema(ctx))
          )
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("All the instances for the requested batchjob")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[
                              Seq[BatchJobInstanceModel]
                            ]].schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }

  private def listInstance(ctx: Context) = {
    new PathItem()
      .get(
        new Operation()
          .operationId("Get batch job instance")
          .description("Get a batch job instance")
          .addTagsItem("batchjobs")
          .addParametersItem(pretty(ctx))
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("batchjobname")
              .description("The name of the batch job to list instance for")
              .schema(stringOpenApi.schema(ctx))
          )
          .addParametersItem(
            new Parameter()
              .in("path")
              .name("instance")
              .description("The name of the batch job instance to list")
              .schema(stringOpenApi.schema(ctx))
          )
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("The instance of the batch job")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[
                              BatchJobInstanceModel
                            ]].schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }

  implicit val batchJobStartResult: ToOpenApiSchema[BatchJobStartResult] =
    product2(BatchJobStartResult)

}
