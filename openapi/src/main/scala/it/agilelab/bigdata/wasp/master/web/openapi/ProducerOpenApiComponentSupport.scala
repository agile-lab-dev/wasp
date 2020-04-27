package it.agilelab.bigdata.wasp.master.web.openapi

import it.agilelab.bigdata.wasp.core.models.ProducerModel

trait ProducerOpenApiComponentSupport extends ProductOpenApi with LangOpenApi with CollectionsOpenApi {
  implicit lazy val producerModel : ToOpenApiSchema[ProducerModel] = product7(ProducerModel)
}
