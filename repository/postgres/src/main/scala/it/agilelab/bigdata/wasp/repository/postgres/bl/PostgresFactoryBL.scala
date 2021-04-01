package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.repository.core.bl._
import it.agilelab.bigdata.wasp.repository.postgres.WaspPostgresDB

class PostgresFactoryBL extends FactoryBL {

  override def getBatchJobBL: BatchJobBL = BatchJobBLImpl(WaspPostgresDB.getDB())

  override def getIndexBL: IndexBL = IndexBLImpl(WaspPostgresDB.getDB())

  override def getPipegraphBL: PipegraphBL = PipegraphBLImpl(WaspPostgresDB.getDB())

  override def getProducerBL: ProducerBL = ProducerBLImpl(WaspPostgresDB.getDB())

  override def getTopicBL: TopicBL = TopicBLImpl(WaspPostgresDB.getDB())

  override def getMlModelBL: MlModelBL = MlModelBLImpl(WaspPostgresDB.getDB())

  override def getWebsocketBL: WebsocketBL = WebsocketBLImpl(WaspPostgresDB.getDB())

  override def getRawBL: RawBL = RawBLImpl(WaspPostgresDB.getDB())

  override def getCdcBL: CdcBL = CdcBLImpl(WaspPostgresDB.getDB())

  override def getKeyValueBL: KeyValueBL =  KeyValueBLImpl(WaspPostgresDB.getDB())

  override def getBatchSchedulersBL: BatchSchedulersBLImpl = BatchSchedulersBLImpl(WaspPostgresDB.getDB())

  override def getDocumentBL: DocumentBL = DocumentBLImpl(WaspPostgresDB.getDB())

  override def getFreeCodeBL: FreeCodeBL =  FreeCodeBLImpl(WaspPostgresDB.getDB())

  override def getProcessGroupBL: ProcessGroupBL = ProcessGroupBLImpl(WaspPostgresDB.getDB())

  override def getConfigManagerBL: ConfigManagerBL = ConfigManagerBLImpl(WaspPostgresDB.getDB())

  override def getSqlSourceBl: SqlSourceBl = SqlSourceBLImpl(WaspPostgresDB.getDB())

  override def getHttpBl: HttpBL = HttpBLImpl(WaspPostgresDB.getDB)

  override def getGenericBL: GenericBL = GenericBLImpl(WaspPostgresDB.getDB())
}
