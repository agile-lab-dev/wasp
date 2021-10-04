package it.agilelab.bigdata.wasp.repository.core.dbModels

import it.agilelab.bigdata.wasp.models.Model

trait FreeCodeDBModel extends Model

case class FreeCodeDBModelV1(override val name : String, code : String) extends FreeCodeDBModel
