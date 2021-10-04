package it.agilelab.bigdata.wasp.repository.core.dbModels

import it.agilelab.bigdata.wasp.models.{HttpCompression, Model}

trait HttpDBModel extends Model

case class HttpDBModelV1(override val name: String,
                         url: String,
                         method: String,
                         headersFieldName: Option[String],
                         valueFieldsNames: List[String],
                         compression: HttpCompression,
                         mediaType: String,
                         logBody: Boolean,
                         structured: Boolean = true
                        ) extends HttpDBModel
