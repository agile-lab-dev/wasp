package it.agilelab.bigdata.wasp.consumers.spark.utils

import scala.tools.reflect.ToolBox
import scala.reflect.runtime.universe

object ToolBoxUtils {

  private lazy val toolBox: ToolBox[universe.type] = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()

  def compileCode[T](code : String) : T = toolBox.eval(toolBox.parse(s"""$code""".stripMargin)).asInstanceOf[T]

}
