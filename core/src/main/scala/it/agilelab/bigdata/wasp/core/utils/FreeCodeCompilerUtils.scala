package it.agilelab.bigdata.wasp.core.utils

import it.agilelab.bigdata.wasp.compiler.utils.{CompletionModel, ErrorModel, FreeCodeCompiler}
import it.agilelab.bigdata.wasp.consumers.spark.strategies.FreeCodeGenerator

trait FreeCodeCompilerUtils {
  def validate(code: String): List[ErrorModel]
  def complete(code: String, position: Int): List[CompletionModel]

}

class FreeCodeCompilerUtilsDefault(freeCodeCompiler: FreeCodeCompiler)
    extends FreeCodeCompilerUtils
    with FreeCodeGenerator
    with AutoCloseable {

  def validate(code: String): List[ErrorModel] =
    freeCodeCompiler.validate(completeWithDefaultCodeAsFunction(code), startRowCode)

  def complete(code: String, position: Int): List[CompletionModel] =
    freeCodeCompiler.complete(completeWithDefaultCodeAsFunction(code), position + startPosition)

  override def close(): Unit = freeCodeCompiler.close()
}
