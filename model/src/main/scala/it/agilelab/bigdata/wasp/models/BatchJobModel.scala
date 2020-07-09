package it.agilelab.bigdata.wasp.models

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import it.agilelab.bigdata.wasp.models.JobStatus.JobStatus
import spray.json._


object JobStatus extends Enumeration {
  type JobStatus = Value

  val PENDING, PROCESSING, SUCCESSFUL, FAILED, STOPPED = Value
}


case class BatchJobModel(override val name: String,
                         description: String,
                         owner: String,
                         system: Boolean,
                         creationTime: Long,
                         etl: BatchETL,
                         exclusivityConfig: BatchJobExclusionConfig = BatchJobExclusionConfig(
                           isFullyExclusive = true,
                           Seq.empty[String])
                        )
  extends Model


case class BatchJobInstanceModel(override val name: String,
                                 instanceOf: String,
                                 startTimestamp: Long,
                                 currentStatusTimestamp: Long,
                                 status: JobStatus,
                                 restConfig: Config = ConfigFactory.empty,
                                 error: Option[String] = None
                                ) extends Model

sealed trait BatchETL {
  val name: String
  val inputs: List[ReaderModel]
  val output: WriterModel
  val group: String
  var isActive: Boolean
}

case class BatchETLModel(name: String,
                         inputs: List[ReaderModel],
                         output: WriterModel,
                         mlModels: List[MlModelOnlyInfo],
                         strategy: Option[StrategyModel],
                         kafkaAccessType: String,
                         group: String = "default",
                         var isActive: Boolean = false) extends BatchETL
object BatchETLModel {
  val TYPE = "BatchETL"
}

case class BatchGdprETLModel(name: String,
                             dataStores: List[DataStoreConf],
                             strategyConfig: String,
                             inputs: List[ReaderModel],
                             output: WriterModel,
                             group: String = "default",
                             var isActive: Boolean = false) extends BatchETL {
  val strategy: GdprStrategyModel = GdprStrategyModel(
    "it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.GdprStrategy",
    dataStores,
    Some(strategyConfig)
  )
}
object BatchGdprETLModel {
  val TYPE = "BatchGdprETL"

  def create(name: String,
             dataStores: List[DataStoreConf],
             strategyConfig: Config,
             inputs: List[ReaderModel],
             output: WriterModel,
             group: String = "default",
             isActive: Boolean = false): BatchGdprETLModel = {
    BatchGdprETLModel(
      name,
      dataStores,
      strategyConfig.root().render(ConfigRenderOptions.concise()),
      inputs,
      output,
      group,
      isActive
    )
  }
}


case class BatchJobExclusionConfig(isFullyExclusive: Boolean, restConfigExclusiveParams: Seq[String])

trait BatchJobJsonSupport extends DefaultJsonProtocol {

  /*def batchJobModelFormat(implicit readerModelFormat: RootJsonFormat[ReaderModel],
                     writerModelFormat: RootJsonFormat[WriterModel],
                     mlModelsFormat: RootJsonFormat[MlModelOnlyInfo],
                     strategyModelFormat: RootJsonFormat[StrategyModel]) = {
    implicit val
  }
*/

  def createBatchETLFormat(implicit batchETLModelFormat: RootJsonFormat[BatchETLModel],
                           batchGdprETLModelFormat: RootJsonFormat[BatchGdprETLModel]): RootJsonFormat[BatchETL] = {

    new RootJsonFormat[BatchETL] {
      override def read(json: JsValue): BatchETL = {
        json.asJsObject.getFields("type") match {
          case Seq(JsString("BatchETLModel")) => json.convertTo[BatchETLModel]
          case Seq(JsString("BatchGdprETLModel")) => json.convertTo[BatchGdprETLModel]
          case _ => throw DeserializationException("Unknown json")
        }
      }

      override def write(obj: BatchETL): JsValue = JsObject(obj match {
        case etl: BatchETLModel => etl.toJson.asJsObject.fields + ("type" -> JsString("BatchETLModel"))
        case gdpr: BatchGdprETLModel => gdpr.toJson.asJsObject.fields + ("type" -> JsString("BatchGdprETLModel"))
      })
    }
  }
}
