package it.agilelab.bigdata.wasp.master.web.openapi
import io.swagger.v3.oas.models.media._
import it.agilelab.bigdata.wasp.models.editor._
import org.json4s.JObject
import spray.json.JsObject

case class ProcessGroupResponseTmp(name: String, processGroupData: String)

trait EditorOpenApiComponentSupport extends LangOpenApi with ProductOpenApi with CollectionsOpenApi with RawModelOpenApiDefinition {

  implicit val jobjectOpenApi: ToOpenApiSchema[JObject] = new ToOpenApiSchema[JObject] {
    override def schema(ctx: Context): Schema[_] = {
      new ObjectSchema()
        .xml(
          new XML().name(classOf[JObject].getSimpleName).namespace("java://" + classOf[JObject].getPackage.getName)
        )
        .name(classOf[JObject].getSimpleName)
    }
  }

  implicit lazy val JsObjectOpenApistringOpenApi: ToOpenApiSchema[JsObject] = stringOpenApi.substituteOf[JsObject]

  implicit lazy val processGroupResponseInstanceOpenApi: ToOpenApiSchema[ProcessGroupResponse] =
    product2(ProcessGroupResponse)

  implicit lazy val nifiStatelessInstanceOpenApi: ToOpenApiSchema[NifiStatelessInstanceModel] =
    product3(NifiStatelessInstanceModel)

  /*
  Pipegraph editor schemas
   */
  implicit lazy val errorDTOInstanceOpenApi: ToOpenApiSchema[ErrorDTO] = product1(ErrorDTO.apply)

  implicit lazy val pipegraphDTOInstanceOpenApi: ToOpenApiSchema[PipegraphDTO] = product4(PipegraphDTO)
  implicit lazy val structuredStreamingETLDTOInstanceOpenApi: ToOpenApiSchema[StructuredStreamingETLDTO] =
    product7(StructuredStreamingETLDTO)

  implicit lazy val freeCodeInstanceOpenApi: ToOpenApiSchema[FreeCodeDTO]           = product3(FreeCodeDTO)
  implicit lazy val flowNifinstanceOpenApi: ToOpenApiSchema[FlowNifiDTO]            = product3(FlowNifiDTO)
  implicit lazy val strategyClassInstanceOpenApi: ToOpenApiSchema[StrategyClassDTO] = product2(StrategyClassDTO)

  implicit lazy val strategyDTOInstanceOpenApi: ToOpenApiSchema[StrategyDTO] =
    new ToOpenApiSchema[StrategyDTO] {
      override def schema(ctx: Context): Schema[_] = {
        val composed      = new ComposedSchema()
        val discriminator = new Discriminator().propertyName("strategyType")
        composed
          .addOneOfItem(shouldBecomeARef(ctx, freeCodeInstanceOpenApi.schema(ctx)))
          .addOneOfItem(shouldBecomeARef(ctx, flowNifinstanceOpenApi.schema(ctx)))
          .addOneOfItem(shouldBecomeARef(ctx, strategyClassInstanceOpenApi.schema(ctx)))
          .discriminator(discriminator)
      }
    }

  implicit lazy val readerDTOInstanceOpenApi: ToOpenApiSchema[ReaderModelDTO] = product4(ReaderModelDTO)
  implicit lazy val writerDTOInstanceOpenApi: ToOpenApiSchema[WriterModelDTO] = product3(WriterModelDTO)

  implicit lazy val rawModelSetupOpenApi: ToOpenApiSchema[RawModelSetupDTO] = product7(RawModelSetupDTO)

  implicit lazy val topicModelDTOOpenApi: ToOpenApiSchema[TopicModelDTO] = product1(TopicModelDTO)
  implicit lazy val indexModelDTOOpenApi: ToOpenApiSchema[IndexModelDTO] = product1(IndexModelDTO)
  implicit lazy val kvModelDTOOpenApi: ToOpenApiSchema[KeyValueModelDTO] = product1(KeyValueModelDTO)
  implicit lazy val rawModelDTOOpenApi: ToOpenApiSchema[RawModelDTO]     = product2(RawModelDTO)

  implicit lazy val datastoreModelDTOInstanceOpenApi: ToOpenApiSchema[DatastoreModelDTO] =
    new ToOpenApiSchema[DatastoreModelDTO] {
      override def schema(ctx: Context): Schema[_] = {
        val composed      = new ComposedSchema()
        val discriminator = new Discriminator().propertyName("modelType")
        composed
          .addOneOfItem(shouldBecomeARef(ctx, topicModelDTOOpenApi.schema(ctx)))
          .addOneOfItem(shouldBecomeARef(ctx, indexModelDTOOpenApi.schema(ctx)))
          .addOneOfItem(shouldBecomeARef(ctx, kvModelDTOOpenApi.schema(ctx)))
          .addOneOfItem(shouldBecomeARef(ctx, rawModelDTOOpenApi.schema(ctx)))
          .discriminator(discriminator)
      }
    }

}
