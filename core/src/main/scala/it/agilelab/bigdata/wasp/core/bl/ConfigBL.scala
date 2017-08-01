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

  def apply(waspDB: WaspDB): Object {val producerBL: ProducerBL; val batchJobBL: BatchJobBL; val mlModelBL: MlModelBL; val pipegraphBL: PipegraphBL; val topicBL: TopicBL; val indexBL: IndexBL} = {
    new Object() {
      val batchJobBL: BatchJobBL = new BatchJobBLImp(waspDB)
      val indexBL: IndexBL = new IndexBLImp(waspDB)
      val pipegraphBL: PipegraphBL = new PipegraphBLImp(waspDB)
      val producerBL: ProducerBL = new ProducerBLImp(waspDB)
      val topicBL: TopicBL = new TopicBLImp(waspDB)
      val mlModelBL: MlModelBL = new MlModelBLImp(waspDB)
      val websocketBL: WebsocketBL = new WebsocketBLImp(waspDB)
      val batchSchedulerBL: BatchSchedulersBL = new BatchSchedulersBLImp(waspDB)
      val rawBL: RawBL = new RawBLImp(waspDB)
      val keyValueBL: KeyValueBL = new KeyValueBLImp(waspDB)
    }
  }

}
