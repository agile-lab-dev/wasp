package it.agilelab.bigdata.wasp.master.web.openapi

import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.models.{ReaderModel, StreamingReaderModel}

trait ReaderModelOpenApiComponentSupport
    extends LangOpenApi
    with ProductOpenApi
    with CollectionsOpenApi
    with EnumOpenApi
    with DataStoreOpenApiComponentSupport {
  val readerModelApply
    : (String, String, DatastoreProduct, Map[String, String]) => ReaderModel =
    (_, _, _, _) => ???

  val streamingReaderModelApply: (String,
                                  String,
                                  DatastoreProduct,
                                  Option[Int],
                                  Map[String, String]) => StreamingReaderModel =
    (_, _, _, _, _) => ???

  implicit lazy val streamingModelOpenApi: ToOpenApiSchema[StreamingReaderModel] =
    product5(streamingReaderModelApply)
  implicit lazy val readerModelOpenApi: ToOpenApiSchema[ReaderModel] = product4(
    readerModelApply
  )
}
