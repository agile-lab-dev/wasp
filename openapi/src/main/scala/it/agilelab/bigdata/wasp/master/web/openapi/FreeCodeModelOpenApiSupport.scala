package it.agilelab.bigdata.wasp.master.web.openapi

import io.swagger.v3.oas.models.media.{ComposedSchema, Schema}
import it.agilelab.bigdata.wasp.compiler.utils.{CompletionModel, ErrorModel}
import it.agilelab.bigdata.wasp.models.{FreeCode, FreeCodeModel}

trait FreeCodeModelOpenApiSupport extends ProductOpenApi with LangOpenApi with CollectionsOpenApi {
  implicit lazy val freeCodeModelOpenApi: ToOpenApiSchema[FreeCodeModel] =
    product2(FreeCodeModel.apply)

  implicit lazy val freeCodeOpenApi: ToOpenApiSchema[FreeCode] =
    product1(FreeCode.apply)


  implicit lazy val completionModelOpenApi: ToOpenApiSchema[CompletionModel] =
    product2(CompletionModel.apply)

  implicit lazy val errorModelOpenApi: ToOpenApiSchema[ErrorModel] =
    product6(ErrorModel.apply)

  sealed trait ValidateResponse

  implicit lazy val insertResponseOpenApi: ToOpenApiSchema[ValidateResponse] = new ToOpenApiSchema[ValidateResponse] {
    override def schema(ctx: Context): Schema[_] = {
      new ComposedSchema()
        .addOneOfItem(
          shouldBecomeARef(ctx, errorModelOpenApi.schema(ctx))
        )
        .addOneOfItem(
          shouldBecomeARef(ctx, stringOpenApi.schema(ctx))
        )
    }
  }


}
