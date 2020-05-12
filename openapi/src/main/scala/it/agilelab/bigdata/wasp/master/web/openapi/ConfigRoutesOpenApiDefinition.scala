package it.agilelab.bigdata.wasp.master.web.openapi

import io.swagger.v3.oas.models.media.{Content, MediaType}
import io.swagger.v3.oas.models.parameters.Parameter
import io.swagger.v3.oas.models.responses.{ApiResponse, ApiResponses}
import io.swagger.v3.oas.models.{Operation, PathItem}
import it.agilelab.bigdata.wasp.core.models.TopicModel
import it.agilelab.bigdata.wasp.core.models.configuration._
import it.agilelab.bigdata.wasp.core.utils.{
  ConnectionConfig,
  ZookeeperConnectionsConfig
}

trait ConfigModelOpenApiComponentSupport
    extends ProductOpenApi
    with OpenApiSchemaSupport
    with CollectionsOpenApi
    with LangOpenApi {

  implicit lazy val elasticConfigOpenApi: ToOpenApiSchema[ElasticConfigModel] =
    product2(ElasticConfigModel)

  implicit lazy val solrConfigOpenApi: ToOpenApiSchema[SolrConfigModel] =
    product2(SolrConfigModel)
  implicit lazy val zookeeperConnectionConfigOpenApi
    : ToOpenApiSchema[ZookeeperConnectionsConfig] = product2(
    ZookeeperConnectionsConfig
  )
  implicit lazy val connectionConfigOpenApi: ToOpenApiSchema[ConnectionConfig] =
    product5(ConnectionConfig)
  implicit lazy val sparkDriverConfigOpenApi
    : ToOpenApiSchema[SparkDriverConfig] =
    product7(SparkDriverConfig)
  implicit lazy val sparkEntryConfigOpenApi: ToOpenApiSchema[SparkEntryConfig] =
    product2(SparkEntryConfig)
  implicit lazy val kryoSerializerConfigOpenApi
    : ToOpenApiSchema[KryoSerializerConfig] = product3(KryoSerializerConfig)
  implicit lazy val sparkBatchConfigOpenApi
    : ToOpenApiSchema[SparkBatchConfigModel] =
    product18(SparkBatchConfigModel)
  implicit lazy val sparkStreamingConfigOpenApi
    : ToOpenApiSchema[SparkStreamingConfigModel] = product21(
    SparkStreamingConfigModel
  )
  implicit lazy val telemetryConfigOpenApi
    : ToOpenApiSchema[TelemetryConfigModel] =
    product4(TelemetryConfigModel)
  implicit lazy val telemetryTopicConfigOpenApi
    : ToOpenApiSchema[TelemetryTopicConfigModel] = product5(
    TelemetryTopicConfigModel
  )
  implicit lazy val kafkaEntryConfigOpenApi: ToOpenApiSchema[KafkaEntryConfig] =
    product2(KafkaEntryConfig)
  implicit lazy val JMXTelemetryConfigOpenApi
    : ToOpenApiSchema[JMXTelemetryConfigModel] = product5(
    JMXTelemetryConfigModel
  )
  implicit lazy val kafkaConfigOpenApi: ToOpenApiSchema[KafkaConfigModel] =
    product13(KafkaConfigModel)

}

trait ConfigRoutesOpenApiDefinition
    extends ConfigModelOpenApiComponentSupport
    with AngularResponseOpenApiComponentSupport {

  def configRoute(ctx: Context): Map[String, PathItem] = {
    Map(
      "/config/sorl" -> solr(ctx),
      "/config/es" -> elastic(ctx),
      "/config/kafka" -> kafka(ctx),
      "/config/telemetry" -> telemetry(ctx),
      "/config/sparkbatch" -> sparkBatch(ctx),
      "/config/sparkstreaming" -> sparkStreaming(ctx)
    )
  }

  private def solr(ctx: Context) = {
    new PathItem()
      .get(
        new Operation()
          .operationId("get-solr-config")
          .description("Retrieves the configuration used to connect to solr")
          .addTagsItem("configuration")
          .addParametersItem(pretty(ctx))
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("solr configuration")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[SolrConfigModel]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }

  private def elastic(ctx: Context) = {
    new PathItem()
      .get(
        new Operation()
          .operationId("get-elastic-config")
          .description("Retrieves the configuration used to connect to elastic")
          .addTagsItem("configuration")
          .addParametersItem(pretty(ctx))
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("elastic configuration")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[ElasticConfigModel]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }
  private def kafka(ctx: Context) = {
    new PathItem()
      .get(
        new Operation()
          .operationId("get-kafka-config")
          .description("Retrieves the configuration used to connect to Kafka")
          .addTagsItem("configuration")
          .addParametersItem(pretty(ctx))
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("kafka configuration")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[KafkaConfigModel]]
                              .schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }

  private def telemetry(ctx: Context) = {
    new PathItem()
      .get(
        new Operation()
          .operationId("get-telemetry-config")
          .description("Retrieves the configuration of the telemetry subsystem")
          .addTagsItem("configuration")
          .addParametersItem(pretty(ctx))
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("telemetry configuration")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[
                              TelemetryConfigModel
                            ]].schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }
  private def sparkBatch(ctx: Context) = {
    new PathItem()
      .get(
        new Operation()
          .operationId("get-spark-batch-config")
          .description("Retrieves the configuration of the Spark Batch context")
          .addTagsItem("configuration")
          .addParametersItem(pretty(ctx))
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("spark batch configuration")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[
                              SparkBatchConfigModel
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

  private def sparkStreaming(ctx: Context) = {
    new PathItem()
      .get(
        new Operation()
          .operationId("get-spark-streaming-config")
          .description("Retrieves the configuration of the Spark Streaming context")
          .addTagsItem("configuration")
          .addParametersItem(pretty(ctx))
          .responses(
            new ApiResponses()
              .addApiResponse(
                "200",
                new ApiResponse()
                  .description("spark streaming configuration")
                  .content(
                    new Content()
                      .addMediaType(
                        "text/json",
                        new MediaType()
                          .schema(
                            ToOpenApiSchema[AngularResponse[
                              SparkStreamingConfigModel
                            ]].schema(ctx)
                          )
                      )
                  )
              )
          )
      )
  }

}
