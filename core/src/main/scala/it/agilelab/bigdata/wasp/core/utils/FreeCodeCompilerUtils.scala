package it.agilelab.bigdata.wasp.core.utils

import it.agilelab.bigdata.wasp.compiler.utils.{CompilerUtils, CompletionModel, ErrorModel}
import it.agilelab.bigdata.wasp.consumers.spark.strategies.FreeCodeGenerator


trait FreeCodeCompilerUtils {
  def validate(code : String) : List[ErrorModel]
  def complete(code : String):List[CompletionModel]

}

object FreeCodeCompilerUtilsDefault extends FreeCodeCompilerUtils with FreeCodeGenerator{

  def validate(code :String): List[ErrorModel] = CompilerUtils.validate(completeWithDefaultCodeAsFunction(code),startRowCode)

  def complete(code : String):List[CompletionModel] = CompilerUtils.complete(completeWithDefaultCodeAsValue(code))

}
