package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.DocumentModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{CdcDBModelV1, DocumentDBModel, DocumentDBModelV1}

object DocumentDBModelMapperSelector extends MapperSelector[DocumentModel, DocumentDBModel]{

  override def select(model : DocumentDBModel) : Mapper[DocumentModel, DocumentDBModel] = {

    model match {
      case _: DocumentDBModelV1 => DocumentMapperV1
      case _ => throw new Exception("There is no available mapper for this DBModel, create one!")
    }
  }

  def applyMap(p: DocumentDBModel) : DocumentModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object DocumentMapperV1 extends Mapper[DocumentModel, DocumentDBModelV1] {
  override val version = "docV1"

  override def fromModelToDBModel(p: DocumentModel): DocumentDBModelV1 = {

    val values = DocumentModel.unapply(p).get
    val makeDBModel = (DocumentDBModelV1.apply _).tupled
    makeDBModel(values)
  }


  override def fromDBModelToModel[B >: DocumentDBModelV1](p: B): DocumentModel = {

    val values = DocumentDBModelV1.unapply(p.asInstanceOf[DocumentDBModelV1]).get
    val makeProducer = (DocumentModel.apply _).tupled
    makeProducer(values)
  }
}
