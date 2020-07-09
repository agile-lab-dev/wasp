package it.agilelab.bigdata.wasp.master.web.openapi

import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.models.WriterModel

trait WriterModelOpenApiComponentSupport
    extends LangOpenApi
    with ProductOpenApi
    with CollectionsOpenApi
    with EnumOpenApi
    with DataStoreOpenApiComponentSupport {
  implicit lazy val writerModelOpenApi: ToOpenApiSchema[WriterModel] = product4(
    writerModelApply
  )
  val writerModelApply
    : (String, String, DatastoreProduct, Map[String, String]) => WriterModel =
    (_, _, _, _) => ???

}
