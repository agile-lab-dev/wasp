package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.SqlSourceModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{SqlSourceDBModel, SqlSourceDBModelV1}

object SqlSourceMapperSelector extends MapperSelector[SqlSourceModel, SqlSourceDBModel]{

  override def select(model : SqlSourceDBModel) : Mapper[SqlSourceModel, SqlSourceDBModel] = {

    model match {
      case _: SqlSourceDBModelV1 => SqlSourceMapperV1
      case _ => throw new Exception("There is no available mapper for this DBModel, create one!")
    }
  }

  def applyMap(p: SqlSourceDBModel) : SqlSourceModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object SqlSourceMapperV1 extends Mapper[SqlSourceModel, SqlSourceDBModelV1] {
  override val version = "sqlV1"

  override def fromModelToDBModel(p: SqlSourceModel): SqlSourceDBModelV1 = {

    val values = SqlSourceModel.unapply(p).get
    val makeDBModel = (SqlSourceDBModelV1.apply _).tupled
    makeDBModel(values)
  }

  override def fromDBModelToModel[B >: SqlSourceDBModelV1](p: B): SqlSourceModel = {

    val values = SqlSourceDBModelV1.unapply(p.asInstanceOf[SqlSourceDBModelV1]).get
    val makeProducer = (SqlSourceModel.apply _).tupled
    makeProducer(values)
  }
}