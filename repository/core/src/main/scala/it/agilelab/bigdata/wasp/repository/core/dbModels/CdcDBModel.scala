package it.agilelab.bigdata.wasp.repository.core.dbModels

import it.agilelab.bigdata.wasp.models.{CdcOptions, Model}

abstract class CdcDBModel extends Model

case class CdcDBModelV1(override val name: String,
                        uri: String,
                        schema: String,
                        options: CdcOptions = CdcOptions.default
                       ) extends CdcDBModel
