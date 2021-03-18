package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl._
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB

class MongoFactoryBL extends FactoryBL {
  def getBatchJobBL: BatchJobBL = new BatchJobBLImp(WaspMongoDB.getDB)
  def getIndexBL: IndexBL = new IndexBLImp(WaspMongoDB.getDB)
  def getPipegraphBL: PipegraphBL = new PipegraphBLImp(WaspMongoDB.getDB)
  def getProducerBL: ProducerBL = new ProducerBLImp(WaspMongoDB.getDB)
  def getTopicBL: TopicBL = new TopicBLImp(WaspMongoDB.getDB)
  def getMlModelBL: MlModelBL = new MlModelBLImp(WaspMongoDB.getDB)
  def getWebsocketBL: WebsocketBL = new WebsocketBLImp(WaspMongoDB.getDB)
  def getRawBL: RawBL = new RawBLImp(WaspMongoDB.getDB)
  def getCdcBL: CdcBL = new CdcBLImp(WaspMongoDB.getDB)
  def getKeyValueBL: KeyValueBL = new KeyValueBLImp(WaspMongoDB.getDB)
  def getBatchSchedulersBL: BatchSchedulersBL = new BatchSchedulersBLImp(WaspMongoDB.getDB)
  def getDocumentBL: DocumentBL = new DocumentBLImpl(WaspMongoDB.getDB)
  def getFreeCodeBL : FreeCodeBL = new FreeCodeBLImpl(WaspMongoDB.getDB)
  def getProcessGroupBL: ProcessGroupBL = new ProcessGroupBLImpl(WaspMongoDB.getDB)
  def getConfigManagerBL : ConfigManagerBL = new ConfigManagerBLImpl(WaspMongoDB.getDB)
  def getSqlSourceBl : SqlSourceBl = new SqlSourceBlImpl(WaspMongoDB.getDB)
  def getHttpBl: HttpBL = new HttpBlImpl(WaspMongoDB.getDB)
}
