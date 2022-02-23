package it.agilelab.bigdata.wasp.repository.core.mappers

import it.agilelab.bigdata.wasp.models.configuration.RestEnrichmentConfigModel
import it.agilelab.bigdata.wasp.models.{DashboardModel, PipegraphInstanceModel, PipegraphModel, StructuredStreamingETLModel}
import it.agilelab.bigdata.wasp.repository.core.dbModels.{PipegraphDBModel, PipegraphDBModelV1, PipegraphDBModelV2, PipegraphInstanceDBModel, PipegraphInstanceDBModelV1}

object PipegraphDBModelMapperSelector extends MapperSelector[PipegraphModel, PipegraphDBModel] {

  override def select(model: PipegraphDBModel): Mapper[PipegraphModel, PipegraphDBModel] = {

    model match {
      case _: PipegraphDBModelV1 => PipegraphMapperV1
      case _: PipegraphDBModelV2 => PipegraphMapperV2
      case o                     => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
    }
  }

  def applyMap(p: PipegraphDBModel): PipegraphModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

// needs to be updated also at PipegraphInstanceDBModelProvider
object PipegraphInstanceDBModelMapperSelector extends MapperSelector[PipegraphInstanceModel, PipegraphInstanceDBModel] {

  override def select(model: PipegraphInstanceDBModel): Mapper[PipegraphInstanceModel, PipegraphInstanceDBModel] = {

    model match {
      case _: PipegraphInstanceDBModelV1 => PipegraphInstanceMapperV1
      case o                             => throw new Exception(s"There is no available mapper for this [$o] DBModel, create one!")
    }
  }

  def applyMap(p: PipegraphInstanceDBModel): PipegraphInstanceModel = {
    val mapper = select(p)
    mapper.fromDBModelToModel(p)
  }
}

object PipegraphMapperV2 extends Mapper[PipegraphModel, PipegraphDBModelV2] {
  override val version = "pipegraphV2"

  def fromModelToDBModel(p: PipegraphModel): PipegraphDBModelV2 = {

    val values      = PipegraphModel.unapply(p).get
    val makeDBModel = (PipegraphDBModelV2.apply _).tupled
    makeDBModel(values)
  }

  override def fromDBModelToModel[B >: PipegraphDBModelV2](p: B): PipegraphModel = {

    val values       = PipegraphDBModelV2.unapply(p.asInstanceOf[PipegraphDBModelV2]).get
    val makeProducer = (PipegraphModel.apply _).tupled
    makeProducer(values)
  }
}

object PipegraphMapperV1 extends Mapper[PipegraphModel, PipegraphDBModelV1] {
  override val version = "pipegraphV1"

  // never used
  private def fromModelToDBModel(p: PipegraphModel): PipegraphDBModelV1 = {
    val values      = PipegraphModel.unapply(p).get
    val makeDBModel = (PipegraphDBModelV1.apply _).tupled
    val t = values match {
      case (
          name: String,
          description: String,
          owner: String,
          isSystem: Boolean,
          creationTime: Long,
          structuredStreamingComponents: List[StructuredStreamingETLModel],
          dashboard: Option[DashboardModel],
          labels: Set[String],
          enrichmentSources: RestEnrichmentConfigModel
        ) =>
        (name, description, owner, isSystem, creationTime, List.empty ,structuredStreamingComponents, List.empty, dashboard, labels, enrichmentSources)
    }
    makeDBModel(t)
  }

  override def fromDBModelToModel[B >: PipegraphDBModelV1](p: B): PipegraphModel = {

    val values       = PipegraphDBModelV1.unapply(p.asInstanceOf[PipegraphDBModelV1]).get
    val makeModel = (PipegraphModel.apply _).tupled
    val t = values match {
      case (
        name: String,
        description: String,
        owner: String,
        isSystem: Boolean,
        creationTime: Long,
        _,
        structuredStreamingComponents: List[StructuredStreamingETLModel],
        _,
        dashboard: Option[DashboardModel],
        labels: Set[String],
        enrichmentSources: RestEnrichmentConfigModel
        ) =>
        (name, description, owner, isSystem, creationTime,structuredStreamingComponents, dashboard, labels, enrichmentSources)
    }
    val pipegraphModel = makeModel(t)
    pipegraphModel
  }
}


object PipegraphInstanceMapperV1 extends Mapper[PipegraphInstanceModel, PipegraphInstanceDBModelV1] {
  override val version = "pipegraphInstanceV1"

  def fromModelToDBModel(p: PipegraphInstanceModel): PipegraphInstanceDBModelV1 = {

    val values      = PipegraphInstanceModel.unapply(p).get
    val makeDBModel = (PipegraphInstanceDBModelV1.apply _).tupled
    makeDBModel(values)
  }

  override def fromDBModelToModel[B >: PipegraphInstanceDBModelV1](p: B): PipegraphInstanceModel = {

    val values       = PipegraphInstanceDBModelV1.unapply(p.asInstanceOf[PipegraphInstanceDBModelV1]).get
    val makeProducer = (PipegraphInstanceModel.apply _).tupled
    makeProducer(values)
  }
}
