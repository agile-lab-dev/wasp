package it.agilelab.bigdata.wasp.master.web.openapi

import it.agilelab.bigdata.wasp.core.models.StrategyModel

trait StrategyModelOpenApiComponentSupport
    extends ProductOpenApi
    with CollectionsOpenApi
    with LangOpenApi {
  implicit lazy val strategyModelOpenApi: ToOpenApiSchema[StrategyModel] =
    product2(StrategyModel.apply)
}
