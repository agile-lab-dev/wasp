package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.SqlSourceModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{SqlSourceDBModel, SqlSourceDBModelV1}

object SqlSourceMapperSelector extends MapperSelector[SqlSourceModel, SqlSourceDBModel]

object SqlSourceMapperV1 extends SimpleMapper[SqlSourceModel, SqlSourceDBModelV1] {
  override val version = "sqlV1"
  override def fromDBModelToModel[B >: SqlSourceDBModelV1](m: B): SqlSourceModel = m match {
    case mm: SqlSourceDBModelV1 => transform[SqlSourceModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }}
