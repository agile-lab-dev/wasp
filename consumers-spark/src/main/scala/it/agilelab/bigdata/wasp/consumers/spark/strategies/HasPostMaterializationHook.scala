package it.agilelab.bigdata.wasp.consumers.spark.strategies

import org.apache.spark.sql.DataFrame

import scala.util.Try


/**
  * Mixin this trait to a strategy that needs an hook after the write phase,
  * the framework will detect this trait and call postMaterialization after the relevant phase
  */
trait HasPostMaterializationHook {
  self: Strategy =>

  /**
    *
    * @param maybeDataframe (optional) output dataframe of the strategy (it is None only if the strategy has no writer)
    * @param maybeError     Some(error) if an error happened during materialization; None otherwise
    * @return the wanted outcome of the strategy. This means that if the maybeError is not None and the hook does not
    *         have a "recovery" logic, it should always return a Failure wrapping the input error.
    */
  def postMaterialization(maybeDataframe: Option[DataFrame], maybeError: Option[Throwable]): Try[Unit]


}
