package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.utils.WaspDB


object ConfigBL {
  lazy val batchJobBL: BatchJobBL = new BatchJobBLImp(WaspDB.getDB)
  lazy val indexBL: IndexBL = new IndexBLImp(WaspDB.getDB)
  lazy val pipegraphBL: PipegraphBL = new PipegraphBLImp(WaspDB.getDB)
  lazy val producerBL: ProducerBL = new ProducerBLImp(WaspDB.getDB)
  lazy val topicBL: TopicBL = new TopicBLImp(WaspDB.getDB)
  lazy val mlModelBL: MlModelBL = new MlModelBLImp(WaspDB.getDB)
  lazy val websocketBL: WebsocketBL = new WebsocketBLImp(WaspDB.getDB)
  lazy val batchSchedulerBL: BatchSchedulersBL = new BatchSchedulersBLImp(WaspDB.getDB)
  lazy val rawBL: RawBL = new RawBLImp(WaspDB.getDB)
  lazy val keyValueBL: KeyValueBL = new KeyValueBLImp(WaspDB.getDB)
  lazy val batchSchedulersBL: BatchSchedulersBL = new BatchSchedulersBLImp(WaspDB.getDB)
  lazy val documentBL: DocumentBL = new DocumentBLImpl(WaspDB.getDB)

}
