package it.agilelab.bigdata.wasp.repository.core.dbModels

import it.agilelab.bigdata.wasp.models.{Model, RawOptions}

trait RawDBModel extends Model

case class RawDBModelV1(override val name: String,
                         uri: String,
                         timed: Boolean = true,
                         schema: String,
                         options: RawOptions = RawOptions.default
                       ) extends RawDBModel