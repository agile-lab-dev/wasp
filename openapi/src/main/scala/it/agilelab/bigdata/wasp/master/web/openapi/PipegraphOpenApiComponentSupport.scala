package it.agilelab.bigdata.wasp.master.web.openapi

import it.agilelab.bigdata.wasp.models.configuration.{RestEnrichmentConfigModel, RestEnrichmentSource}
import it.agilelab.bigdata.wasp.models.{DashboardModel, LegacyStreamingETLModel, PipegraphInstanceModel, PipegraphModel, PipegraphStatus, RTModel, StructuredStreamingETLModel}

trait PipegraphOpenApiComponentSupport
    extends LangOpenApi
    with ProductOpenApi
    with CollectionsOpenApi
    with EnumOpenApi
    with ReaderModelOpenApiComponentSupport
    with WriterModelOpenApiComponentSupport
    with StrategyModelOpenApiComponentSupport
    with MlModelOnlyInfoComponentSupport {

   implicit lazy val pipegraphStatusOpenApi: ToOpenApiSchema[PipegraphStatus.Value] =
    enumOpenApi(PipegraphStatus)
   implicit lazy val pipegraphInstanceOpenApi
    : ToOpenApiSchema[PipegraphInstanceModel] = product8(PipegraphInstanceModel)

   implicit lazy val pipegraphOpenApi: ToOpenApiSchema[PipegraphModel] = product11(
    PipegraphModel
  )

   implicit lazy val rtModelOpenApi : ToOpenApiSchema[RTModel] = product5(RTModel)

   implicit lazy val dashboardOpenApi : ToOpenApiSchema[DashboardModel] = product2(DashboardModel)

   implicit lazy val legacyStreamingOpenApiModel
    : ToOpenApiSchema[LegacyStreamingETLModel] = product8(
    LegacyStreamingETLModel.apply
  )

   implicit lazy val structuredStreamingOpenApiModel
    : ToOpenApiSchema[StructuredStreamingETLModel] = product9(
    StructuredStreamingETLModel
  )

  implicit lazy val enrichmentSourcesOpenApiModel
    : ToOpenApiSchema[RestEnrichmentSource] = product3(
    RestEnrichmentSource
  )

  implicit lazy val restEnrichmentConfigModelOpenApiModel
    : ToOpenApiSchema[RestEnrichmentConfigModel] = product1(
    RestEnrichmentConfigModel
  )
}
