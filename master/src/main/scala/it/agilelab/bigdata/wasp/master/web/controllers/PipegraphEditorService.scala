package it.agilelab.bigdata.wasp.master.web.controllers

import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.{GenericIndexProduct, GenericKeyValueProduct, GenericTopicProduct, RawProduct}
import it.agilelab.bigdata.wasp.models.{PipegraphModel, StrategyModel}
import it.agilelab.bigdata.wasp.models.editor.ErrorDTO
import it.agilelab.bigdata.wasp.repository.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.utils.FreeCodeCompilerUtils

trait PipegraphEditorService {
  def checkPipegraphName(name: String): Option[ErrorDTO]
  def checkIOByName(name: String, datastore: DatastoreProduct): Option[ErrorDTO]
  def checkStrategy(strategy: StrategyModel): List[ErrorDTO]

  def insertPipegraphModel(model: PipegraphModel): Unit

  def validatePipegraphModel(model: PipegraphModel): List[ErrorDTO] = {
    checkPipegraphName(model.name).map(x => List(x)).getOrElse(List.empty) ++
      model.structuredStreamingComponents.flatMap(x =>
        checkIOByName(x.streamingInput.name, x.streamingInput.datastoreProduct) ++
          checkIOByName(x.streamingOutput.name, x.streamingOutput.datastoreProduct) ++
           x.strategy.map(s => checkStrategy(s)).getOrElse(List.empty)
      )
  }
}

class DefaultPipegraphEditorService(val utils: FreeCodeCompilerUtils) extends PipegraphEditorService {

  override def checkPipegraphName(name: String): Option[ErrorDTO] =
    ConfigBL.pipegraphBL.getByName(name).map(_ => ErrorDTO.alreadyExists("Pipegraph", name))

  override def checkIOByName(name: String, datastore: DatastoreProduct): Option[ErrorDTO] = {
    datastore match {
      case GenericIndexProduct => ConfigBL.indexBL.getByName(name).map(_ => ErrorDTO.alreadyExists("Streaming IO [index]", name))
      case GenericKeyValueProduct => ConfigBL.keyValueBL.getByName(name).map(_ => ErrorDTO.alreadyExists("Streaming IO [key-value]", name))
      case GenericTopicProduct => ConfigBL.topicBL.getByName(name).map(_ => ErrorDTO.alreadyExists("Streaming IO [topic]", name))
      case RawProduct => ConfigBL.rawBL.getByName(name).map(_ => ErrorDTO.alreadyExists("Streaming IO [raw]", name))
      case _ => Some(ErrorDTO.unknownArgument("Datastore product", datastore.getActualProductName))
    }
  }

  override def checkStrategy(strategy: StrategyModel): List[ErrorDTO] =
    strategy match {
      case StrategyModel("it.agilelab.bigdata.wasp.consumers.spark.strategies.FreeCodeStrategy", x) =>
        utils.validate(x.get).map(e => ErrorDTO(e.toString()))
      case _ => List.empty
    }

  override def insertPipegraphModel(model: PipegraphModel): Unit =
    ConfigBL.pipegraphBL.insert(model)
}