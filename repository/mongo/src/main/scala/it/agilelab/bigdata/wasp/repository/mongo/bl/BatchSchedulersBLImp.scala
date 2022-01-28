package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.BatchSchedulersBL
import it.agilelab.bigdata.wasp.models.BatchSchedulerModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{BatchSchedulerDBModel, BatchSchedulerDBModelV1}
import it.agilelab.bigdata.wasp.repository.core.mappers.BatchSchedulerMapperV1.transform
import it.agilelab.bigdata.wasp.repository.core.mappers.BatchSchedulersMapperSelector.factory
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.bson.BsonBoolean


class BatchSchedulersBLImp(waspDB: WaspMongoDB) extends BatchSchedulersBL {

  def getActiveSchedulers(isActive: Boolean = true): Seq[BatchSchedulerModel] = {
    waspDB.getAllDocumentsByField[BatchSchedulerDBModel]("isActive", new BsonBoolean(isActive)).map(factory)
  }


  override def persist(schedulerModel: BatchSchedulerModel): Unit = {
    waspDB.insert[BatchSchedulerDBModel](transform[BatchSchedulerDBModelV1](schedulerModel))
  }
}
