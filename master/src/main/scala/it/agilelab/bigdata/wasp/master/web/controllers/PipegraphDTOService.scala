package it.agilelab.bigdata.wasp.master.web.controllers

import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.models.{PipegraphModel, StrategyModel, StreamingReaderModel, StructuredStreamingETLModel, WriterModel}
import it.agilelab.bigdata.wasp.models.editor.{PipegraphDTO, RawDataDTO, StrategyDTO, StreamingOutputDTO, StructuredStreamingETLDTO}

trait PipegraphDTOService {
  /**
    * Set:
    * isSystem: Boolean
    * creationTime: Long
    *
    * Not used:
    * legacyStreamingComponents: List[LegacyStreamingETLModel]
    * rtComponents: List[RTModel]
    * dashboard: Option[DashboardModel] = None
    *
    * calls:
    * convertDTOStreamingComponents
    *
    * @param DTOPipegraph
    * @return
    */
  def convertPipegraphDTO(DTOPipegraph: PipegraphDTO): Either[ErrorDTO, PipegraphModel]

  /**
    * Set:
    * Not used:
    * staticInputs: List[ReaderModel], Not used
    * mlModels: List[MlModelOnlyInfo], Not used
    * options: Map[String, String] = Map.empty, Not used
    *
    * Calls:
    * convertStreamingInput
    * convertStreamingOutput
    * convertStrategy
    *
    * @param structuredStreamingComponent
    * @return
    */
  def convertStructuredStreamingETLDTO(
      structuredStreamingComponent: StructuredStreamingETLDTO
  ): Either[ErrorDTO, StructuredStreamingETLModel]

  /**
    * if valid streamingInput set:
    * datastoreModelName: String
    * datastoreProduct: DatastoreProduct
    *
    * set default:
    * rateLimit: Option[Int]
    *
    * Not used:
    * options: Map[String, String])
    *
    * @param streamingInput
    * @return
    */
  def convertStreamingInput(streamingInput: String): Either[ErrorDTO, StreamingReaderModel]

  /**
    * if valid destinationType, name or rawData set:
    * datastoreModelName: String
    * datastoreProduct: DatastoreProduct
    *
    * Not used:
    * options: Map[String, String]
    *
    * @param streamingOutput
    * @return
    */
  def convertStreamingOutputDTO(streamingOutput: StreamingOutputDTO): Either[ErrorDTO, WriterModel]

  /**
    * ???
    * @param rawData
    * @return
    */
  def convertRawDataDTO(rawData: RawDataDTO): Either[ErrorDTO, DatastoreProduct]

  /**
    * Generate:
    * configuration: Config
    * Class and Class name
    *
    *
    * @param strategy
    * @return
    */
  def convertStrategyDTO(strategy: StrategyDTO): Either[ErrorDTO, StrategyModel]
}

case class ErrorDTO(
    msg: String
)
