package it.agilelab.bigdata.wasp.core.models

import reactivemongo.bson.{BSONDocument, BSONObjectID}


case class DashboardModel(url: String, needsFilterBox: Boolean)

case class StrategyModel(className: String, configuration: Option[BSONDocument] = None)

case class ETLModel(name: String, inputs: List[ReaderModel], output: WriterModel, mlModels: List[MlModelOnlyInfo], strategy: Option[StrategyModel], kafkaAccessType: String, group: String = "default", var isActive: Boolean = true)

case class RTModel(name: String, inputs: List[ReaderModel], var isActive: Boolean = true, strategy: Option[StrategyModel] = None, endpoint: Option[WriterModel] = None)

case class WriterModel(id: BSONObjectID, name: String, writerType: WriteType)

case class WriteType(wtype: String, product: String)

case class ReaderModel(id: BSONObjectID, name: String, readerType: String)


object WriterModel {

  def IndexWriter(id_index: BSONObjectID, name: String) = WriterModel(id_index, name, WriteType("index", "elastic"))

  def IndexWriter(id_index: BSONObjectID, name: String, product: String) = WriterModel(id_index, name, WriteType("index", product))

  def keyValueWriter(id_index: BSONObjectID, name: String, product: String) = WriterModel(id_index, name, WriteType("hbase", "hbase"))

  def TopicWriter(id_topic: BSONObjectID, name: String) = WriterModel(id_topic, name, WriteType("topic", "kafka"))

  def WebSocketWriter(id_websocket: BSONObjectID, name: String) = WriterModel(id_websocket, name, WriteType("websocket", "websocket"))

  def RawWriter(id_raw: BSONObjectID, name: String) = WriterModel(id_raw, name, WriteType("raw", "raw"))
}

object ReaderModel {

  def IndexReader(id_index: BSONObjectID, name: String) = ReaderModel(id_index, name, "index")

  def TopicReader(id_topic: BSONObjectID, name: String) = ReaderModel(id_topic, name, "topic")

  def WebSocketReader(id_websocket: BSONObjectID, name: String) = ReaderModel(id_websocket, name, "websocket")

  def RawReader(id_raw: BSONObjectID, name: String) = ReaderModel(id_raw, name, "raw")
}

case class PipegraphModel(override val name: String,
                          description: String,
                          owner: String,
                          system: Boolean,
                          creationTime: Long,
                          etl: List[ETLModel],
                          rt: List[RTModel],
                          dashboard: Option[DashboardModel] = None,
                          var isActive: Boolean = true,
                          _id: Option[BSONObjectID] = None) extends Model

object ETLModel {
  val KAFKA_ACCESS_TYPE_DIRECT = "direct"
  val KAFKA_ACCESS_TYPE_RECEIVED_BASED = "receiver-based"
}