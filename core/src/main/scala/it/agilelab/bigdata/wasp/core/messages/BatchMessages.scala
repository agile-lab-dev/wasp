package it.agilelab.bigdata.wasp.core.messages

import com.typesafe.config.Config
import it.agilelab.bigdata.wasp.core.WaspMessage


object BatchMessages {

  /**
    * Trait marking messages as being related to batch processing.
    */
  sealed trait BatchMessage extends WaspMessage


  /**
    * Message sent to batch consumer guardian to request start of a [[it.agilelab.bigdata.wasp.core.models.BatchJobModel]].
    *
    * @param name The name of the [[it.agilelab.bigdata.wasp.core.models.BatchJobModel]] to start
    */
  case class StartBatchJob(name: String, restConfig: Config) extends BatchMessage

  /**
    * Trait marking messages as being result of the [[StartBatchJob]] request.
    */
  sealed trait StartBatchJobResult extends BatchMessage

  /**
    * Successful response to [[StartBatchJob]]
    * @param name The name of the [[it.agilelab.bigdata.wasp.core.models.BatchJobModel]] started
    */
  case class StartBatchJobResultSuccess(name: String) extends StartBatchJobResult

  /**
    * Failure response to [[StartBatchJob]]
    * @param name The name of the [[it.agilelab.bigdata.wasp.core.models.BatchJobModel]] that did not start.
    * @param error
    */
  case class StartBatchJobResultFailure(name: String, error: String) extends StartBatchJobResult

  /**
    * Message sent to batch consumer guardian to request stop of a [[it.agilelab.bigdata.wasp.core.models.BatchJobModel]].
    * @param name
    */
  case class StopBatchJob(name: String) extends BatchMessage

  /**
    * Trait marking messages as being result of the [[StopBatchJob]] request.
    */
  sealed trait StopBatchJobResult extends BatchMessage

  /**
    * Successful response to [[StopBatchJob]]
    * @param name The name of the [[it.agilelab.bigdata.wasp.core.models.BatchJobModel]] stopped
    */
  case class StopBatchJobResultSuccess(name: String) extends StopBatchJobResult

  /**
    * Failure response to [[StopBatchJob]]
    * @param name The name of the [[it.agilelab.bigdata.wasp.core.models.BatchJobModel]] stopped
    */
  case class StopBatchJobResultFailure(name: String, error: String) extends StopBatchJobResult

}