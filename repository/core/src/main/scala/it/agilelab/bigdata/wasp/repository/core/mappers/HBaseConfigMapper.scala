package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.configuration.HBaseConfigModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{HBaseConfigDBModel, HBaseConfigDBModelV1}

object HBaseConfigMapperSelector extends MapperSelector[HBaseConfigModel, HBaseConfigDBModel]

object HBaseConfigMapperV1 extends SimpleMapper[HBaseConfigModel, HBaseConfigDBModelV1] {
  override val version = "hbaseConfigV1"
  override def fromDBModelToModel[B >: HBaseConfigDBModelV1](m: B): HBaseConfigModel = m match {
    case mm: HBaseConfigDBModelV1 => transform[HBaseConfigModel](mm)
    case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
  }}
