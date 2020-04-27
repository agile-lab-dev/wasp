package it.agilelab.bigdata.wasp.master.web.openapi

import it.agilelab.bigdata.wasp.master.web.openapi.ResultIndicator.ResultIndicator

trait AngularResponseOpenApiComponentSupport
    extends ProductOpenApi
    with EnumOpenApi {
  implicit def angularResponseOpenApi[T: ToOpenApiSchema]
    : ToOpenApiSchema[AngularResponse[T]] = product2(AngularResponse.apply[T])

  implicit val resultEnumOpenApi: ToOpenApiSchema[ResultIndicator] =
    enumOpenApi(ResultIndicator)
}
