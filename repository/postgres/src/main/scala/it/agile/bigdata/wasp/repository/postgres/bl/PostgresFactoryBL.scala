package it.agile.bigdata.wasp.repository.postgres.bl

import it.agile.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agilelab.bigdata.wasp.repository.core.bl._

class PostgresFactoryBL extends FactoryBL {

  override def getBatchJobBL: BatchJobBL = BatchJobBLImpl(WaspPostgresDB.getDB())

  override def getIndexBL: IndexBL = IndexBLImpl(WaspPostgresDB.getDB())

  override def getPipegraphBL: PipegraphBL = ???

  override def getProducerBL: ProducerBL = ProducerBLImpl(WaspPostgresDB.getDB())

  override def getTopicBL: TopicBL = TopicBLImpl(WaspPostgresDB.getDB())

  override def getMlModelBL: MlModelBL = ???

  override def getWebsocketBL: WebsocketBL = ???

  override def getRawBL: RawBL = RawBLImpl(WaspPostgresDB.getDB())

  override def getKeyValueBL: KeyValueBL =  KeyValueBLImpl(WaspPostgresDB.getDB())

  override def getBatchSchedulersBL: BatchSchedulersBLImpl = BatchSchedulersBLImpl(WaspPostgresDB.getDB())

  override def getDocumentBL: DocumentBL = ???

  override def getFreeCodeBL: FreeCodeBL =  FreeCodeBLImpl(WaspPostgresDB.getDB())

  override def getProcessGroupBL: ProcessGroupBL = ProcessGroupBLImpl(WaspPostgresDB.getDB())

  override def getConfigManagerBL: ConfigManagerBL = ConfigManagerBLImpl(WaspPostgresDB.getDB())

  override def getSqlSourceBl: SqlSourceBl = SqlSourceBLImpl(WaspPostgresDB.getDB())
}
