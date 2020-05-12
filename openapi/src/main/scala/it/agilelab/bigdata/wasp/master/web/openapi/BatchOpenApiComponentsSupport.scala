package it.agilelab.bigdata.wasp.master.web.openapi

import com.typesafe.config.Config
import io.swagger.v3.oas.models.media.{ComposedSchema, Discriminator, Schema}
import it.agilelab.bigdata.wasp.core.models._

trait BatchOpenApiComponentsSupport
    extends LangOpenApi
    with ProductOpenApi
    with CollectionsOpenApi
    with EnumOpenApi
    with ReaderModelOpenApiComponentSupport
    with WriterModelOpenApiComponentSupport
    with StrategyModelOpenApiComponentSupport
    with MlModelOnlyInfoComponentSupport
    with OpenApiSchemaSupport
with KeyValueModelOpenApiDefinition {

  implicit lazy val typesafeConfig: ToOpenApiSchema[Config] =
    objectOpenApi[Config]
  implicit lazy val jobStatusOpenApi: ToOpenApiSchema[JobStatus.JobStatus] =
    enumOpenApi(JobStatus)
  implicit lazy val batchJobInstanceOpenApi
    : ToOpenApiSchema[BatchJobInstanceModel] =
    product7(BatchJobInstanceModel)


  implicit lazy val batchExclusivityConfigOpenApi
    : ToOpenApiSchema[BatchJobExclusionConfig] = product2(
    BatchJobExclusionConfig
  )
  implicit lazy val batchJobModelOpenApi: ToOpenApiSchema[BatchJobModel] =
    product7(BatchJobModel)

  implicit lazy val batchEltModelOpenApi: ToOpenApiSchema[BatchETLModel] =
    product8(BatchETLModel.apply)
      .mapSchema((c, x) => x.addProperties("type", stringOpenApi.schema(c)))

  implicit lazy val datastoreConfOpenApi: ToOpenApiSchema[DataStoreConf] =
    new ToOpenApiSchema[DataStoreConf] {
      override def schema(ctx: Context): Schema[_] = {
        val composed = new ComposedSchema()
        val discriminator = new Discriminator().propertyName("type")
        composed
          .addOneOfItem(
            shouldBecomeARef(ctx, rawDataStoreConfOpenApi.schema(ctx))
          )
          .addOneOfItem(
            shouldBecomeARef(ctx, keyValueDataStoreConfOpenApi.schema(ctx))
          )
          .discriminator(discriminator)
      }
    }

  implicit lazy val keyValueMatchingStrategy
    : ToOpenApiSchema[KeyValueMatchingStrategy] =
    new ToOpenApiSchema[KeyValueMatchingStrategy] {
      override def schema(ctx: Context): Schema[_] = {
        val composed = new ComposedSchema()
        val discriminator = new Discriminator().propertyName("type")
        composed
          .addOneOfItem(
            shouldBecomeARef(ctx, exactKeyValueMatchingStrategy.schema(ctx))
          )
          .addOneOfItem(
            shouldBecomeARef(ctx, timeBoundKeyValueMatchingStrategy.schema(ctx))
          )
          .addOneOfItem(
            shouldBecomeARef(ctx, prefixKeyValueMatchingStrategy.schema(ctx))
          )
          .discriminator(discriminator)
      }
    }

  implicit lazy val timeBasedBetweenPartitionPruningStrategy
    : ToOpenApiSchema[TimeBasedBetweenPartitionPruningStrategy] = product4(
    TimeBasedBetweenPartitionPruningStrategy.apply
  )

  implicit lazy val noPartitionPruningStrategy
    : ToOpenApiSchema[NoPartitionPruningStrategy] =
    objectOpenApi.mapSchema((ctx, s) => s.name("ExactKeyValueMatchingStrategy"))

  implicit lazy val partitionPruningStrategy
    : ToOpenApiSchema[PartitionPruningStrategy] =
    new ToOpenApiSchema[PartitionPruningStrategy] {
      override def schema(ctx: Context): Schema[_] = {
        val composed = new ComposedSchema()
        val discriminator = new Discriminator().propertyName("type")
        composed
          .addOneOfItem(
            shouldBecomeARef(ctx, noPartitionPruningStrategy.schema(ctx))
          )
          .addOneOfItem(
            shouldBecomeARef(
              ctx,
              timeBasedBetweenPartitionPruningStrategy.schema(ctx)
            )
          )
          .discriminator(discriminator)
      }
    }

  implicit lazy val rawDataStoreConfOpenApi: ToOpenApiSchema[RawDataStoreConf] =
    product6(RawDataStoreConf.apply)

  implicit lazy val rawModelOpenApi: ToOpenApiSchema[RawModel] = product5(
    RawModel.apply
  )

  implicit lazy val rawOptionOpenApi: ToOpenApiSchema[RawOptions] = product4(
    RawOptions.apply
  )
  implicit lazy val exactKeyValueMatchingStrategy
    : ToOpenApiSchema[ExactKeyValueMatchingStrategy] =
    objectOpenApi.mapSchema((ctx, s) => s.name("ExactKeyValueMatchingStrategy"))

  implicit lazy val prefixKeyValueMatchingStrategy
    : ToOpenApiSchema[PrefixKeyValueMatchingStrategy] = objectOpenApi.mapSchema(
    (ctx, s) => s.name("PrefixKeyValueMatchingStrategy")
  )
  implicit lazy val timeBoundKeyValueMatchingStrategy
    : ToOpenApiSchema[PrefixAndTimeBoundKeyValueMatchingStrategy] = product3(
    PrefixAndTimeBoundKeyValueMatchingStrategy.apply
  )

  implicit lazy val exactRawMatchingStrategyOpenApi
    : ToOpenApiSchema[ExactRawMatchingStrategy] = product1(
    ExactRawMatchingStrategy.apply
  )
  implicit lazy val prefixRawMatchingStrategyOpenApi
    : ToOpenApiSchema[PrefixRawMatchingStrategy] = product1(
    PrefixRawMatchingStrategy.apply
  )

  implicit lazy val containsRawMatchingStrategyOpenApi
    : ToOpenApiSchema[ContainsRawMatchingStrategy] = product1(
    ContainsRawMatchingStrategy.apply
  )

  implicit lazy val keyValueDataStoreConfOpenApi
    : ToOpenApiSchema[KeyValueDataStoreConf] = product4(
    KeyValueDataStoreConf.apply
  )

  implicit lazy val rawMatchingStrategy: ToOpenApiSchema[RawMatchingStrategy] =
    new ToOpenApiSchema[RawMatchingStrategy] {
      override def schema(ctx: Context): Schema[_] = {
        val composed = new ComposedSchema()
        val discriminator = new Discriminator().propertyName("type")
        composed
          .addOneOfItem(
            shouldBecomeARef(ctx, exactRawMatchingStrategyOpenApi.schema(ctx))
          )
          .addOneOfItem(
            shouldBecomeARef(ctx, prefixRawMatchingStrategyOpenApi.schema(ctx))
          )
          .addOneOfItem(
            shouldBecomeARef(
              ctx,
              containsRawMatchingStrategyOpenApi.schema(ctx)
            )
          )
          .discriminator(discriminator)
      }
    }

  implicit lazy val gdprBatchEltModelOpenApi
    : ToOpenApiSchema[BatchGdprETLModel] =
    product7(BatchGdprETLModel.apply)
      .mapSchema((c, x) => x.addProperties("type", stringOpenApi.schema(c)))

  implicit lazy val batchEtlOpenApi: ToOpenApiSchema[BatchETL] =
    new ToOpenApiSchema[BatchETL] {
      override def schema(ctx: Context): Schema[_] = {
        val composed = new ComposedSchema()
        val discriminator = new Discriminator().propertyName("type")
        composed
          .addOneOfItem(shouldBecomeARef(ctx, batchEltModelOpenApi.schema(ctx)))
          .addOneOfItem(
            shouldBecomeARef(ctx, gdprBatchEltModelOpenApi.schema(ctx))
          )
          .discriminator(discriminator)
      }
    }
}
