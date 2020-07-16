package it.agilelab.bigdata.wasp.models.editor

import it.agilelab.bigdata.wasp.models.{PipegraphModel, ReaderModel, StrategyModel, StreamingReaderModel, StructuredStreamingETLModel, WriterModel}

import it.agilelab.bigdata.wasp.datastores.DatastoreProduct

/**
  * Pipegraph data transfer object
  *
  * @param name name of the pipegraph
  * @param description description of the pipegraph
  * @param owner owner of the pipegraph
  * @param structuredStreamingComponents components describing processing built on Spark Structured Streaming
  *
  */
case class PipegraphDTO(
    name: String,
    description: String,
    owner: Option[String],
    structuredStreamingComponents: List[StructuredStreamingETLDTO]
) {
  def toPipegraphModel: Either[List[ErrorDTO], PipegraphModel] = {
    val errs: List[ErrorDTO] =
      structuredStreamingComponents.flatMap(_.toStructuredStreamingETLModel.fold(x => x, _ => List.empty))

    if (errs.nonEmpty) Left(errs)
    else
      Right(
        PipegraphModel(
          name,
          description,
          owner.getOrElse("ui"),
          isSystem = false,
          creationTime = System.currentTimeMillis(),
          legacyStreamingComponents = List.empty,
          structuredStreamingComponents = structuredStreamingComponents.map(_.toStructuredStreamingETLModel.right.get),
          rtComponents = List.empty
        )
      )

  }
}

/**
  * StructuredStreamingETLModel data transfer object
  *
  * @param name unique name of the processing component
  * @param group group of which the processing component is part
  * @param streamingInput streaming input unique name
  * @param streamingOutput streaming output definition
  * @param strategy strategy model that defines the processing
  * @param triggerIntervalMs trigger interval to use, in milliseconds
  */
case class StructuredStreamingETLDTO(
    name: String,
    group: String,
    streamingInput: ReaderModelDTO,
    streamingOutput: WriterModelDTO,
    strategy: StrategyDTO,
    triggerIntervalMs: Long,
    options: Map[String, String]
) {
  def toStructuredStreamingETLModel: Either[List[ErrorDTO], StructuredStreamingETLModel] = {
    val errs: List[ErrorDTO] =
      streamingInput.toReaderModel.fold(x => x, _ => List.empty) ++
        streamingOutput.toWriterModel.fold(x => x, _ => List.empty) ++
        strategy.toStrategyModel.fold(x => x, _ => List.empty)

    if (errs.nonEmpty) Left(errs)
    else
      Right(
        StructuredStreamingETLModel(
          name,
          group,
          streamingInput = streamingInput.toReaderModel.right.get,
          staticInputs = List.empty, // !
          streamingOutput = streamingOutput.toWriterModel.right.get,
          mlModels = List.empty, // !
          strategy = Some(strategy.toStrategyModel.right.get),
          triggerIntervalMs = Some(triggerIntervalMs),
          options = options
        )
      )
  }
}

/**
  * WriterModel data transfer object
  */
sealed trait StreamingIODTO {
  protected def mapProduct(product: String): Either[List[ErrorDTO], DatastoreProduct] = product match {
    case "Topic"    => Right(DatastoreProduct.GenericTopicProduct)
    case "Index"    => Right(DatastoreProduct.GenericIndexProduct)
    case "KeyValue" => Right(DatastoreProduct.GenericKeyValueProduct)
    case "RawData"  => Right(DatastoreProduct.RawProduct)
    case _          => Left(List(ErrorDTO.unknownArgument(s"Datastore product", product)))
  }
}

case class WriterModelDTO(
    name: String,
    datastoreModelName: String,
    datastoreProduct: String,
    options: Map[String, String]
) extends StreamingIODTO {
  def toWriterModel: Either[List[ErrorDTO], WriterModel] =
    mapProduct(datastoreProduct).right.map(x => WriterModel(name, datastoreModelName, x, options))
}

case class ReaderModelDTO(
    name: String,
    datastoreModelName: String,
    datastoreProduct: String,
    options: Map[String, String],
    rateLimit: Option[Int]
) extends StreamingIODTO {
  def toReaderModel: Either[List[ErrorDTO], StreamingReaderModel] = {
    mapProduct(datastoreProduct).right.map(x => StreamingReaderModel(name, datastoreModelName, x, rateLimit, options))
  }
}

/**
  * Strategy data transfer object
  */
sealed trait StrategyDTO {
  def toStrategyModel: Either[List[ErrorDTO], StrategyModel]
}

object StrategyDTO {
  val nifiType     = "nifi"
  val codebaseType = "codebase"
  val freecodeType = "freecode"
}

case class FreeCodeDTO(code: String) extends StrategyDTO {
  override def toStrategyModel: Either[List[ErrorDTO], StrategyModel] =
     Right(StrategyModel("it.agilelab.bigdata.wasp.consumers.spark.strategies.FreeCodeStrategy", Some(code)))
}
case class FlowNifiDTO(processGroup: String) extends StrategyDTO {
  override def toStrategyModel: Either[List[ErrorDTO], StrategyModel] =
    Right(StrategyModel("it.agilelab.bigdata.wasp.spark.plugins.nifi.NifiStrategy", Some(processGroup)))
}
case class StrategyClassDTO(className: String) extends StrategyDTO {
  override def toStrategyModel: Either[List[ErrorDTO], StrategyModel] =
    Right(StrategyModel(className))
}
