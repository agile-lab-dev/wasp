package it.agilelab.bigdata.wasp.repository.core.bl

import it.agilelab.bigdata.wasp.repository.core.db.RepositoriesFactory.service.{getFactoryBL=>factory}

object ConfigBL {

  lazy val batchJobBL: BatchJobBL = factory.getBatchJobBL
  lazy val indexBL: IndexBL = factory.getIndexBL
  lazy val pipegraphBL: PipegraphBL = factory.getPipegraphBL
  lazy val producerBL: ProducerBL = factory.getProducerBL
  lazy val topicBL: TopicBL = factory.getTopicBL
  lazy val mlModelBL: MlModelBL = factory.getMlModelBL
  lazy val websocketBL: WebsocketBL = factory.getWebsocketBL
  lazy val batchSchedulerBL: BatchSchedulersBL = factory.getBatchSchedulersBL
  lazy val rawBL: RawBL = factory.getRawBL
  lazy val keyValueBL: KeyValueBL = factory.getKeyValueBL
  lazy val batchSchedulersBL: BatchSchedulersBL = factory.getBatchSchedulersBL
  lazy val documentBL: DocumentBL = factory.getDocumentBL
  lazy val freeCodeBL : FreeCodeBL = factory.getFreeCodeBL
  lazy val processGroupBL: ProcessGroupBL = factory.getProcessGroupBL
  lazy val configManagerBL : ConfigManagerBL = factory.getConfigManagerBL
  lazy val dBConfigBL :  DBConfigBL = factory.getDBConfigBL
  lazy val sqlSourceBl : SqlSourceBl = factory.getSqlSourceBl
}



trait FactoryBL {

  def getBatchJobBL: BatchJobBL
  def getIndexBL: IndexBL
  def getPipegraphBL: PipegraphBL
  def getProducerBL: ProducerBL
  def getTopicBL: TopicBL
  def getMlModelBL: MlModelBL
  def getWebsocketBL: WebsocketBL
  def getRawBL: RawBL
  def getKeyValueBL: KeyValueBL
  def getBatchSchedulersBL: BatchSchedulersBL
  def getDocumentBL: DocumentBL
  def getFreeCodeBL : FreeCodeBL
  def getProcessGroupBL: ProcessGroupBL
  def getConfigManagerBL : ConfigManagerBL
  def getDBConfigBL :  DBConfigBL
  def getSqlSourceBl : SqlSourceBl

}
