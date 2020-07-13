package it.agilelab.bigdata.wasp.compiler.utils

import it.agilelab.bigdata.wasp.models.{CompletionModel, ErrorModel}

class FreeCodeCompiler(compilerPool: CompilerPool) extends AutoCloseable {

  private val startClass = """object Compilation {""".stripMargin
  private val endClass   = """}""".stripMargin

  def validate(code: String, startPosition: Int = 0): List[ErrorModel] =
    compilerPool.use { compiler =>
      val completeClass =
        s"""$startClass
         |$code
         |$endClass""".stripMargin
      val endPosition = completeClass.length

      compiler.scopeCompletion(s"$completeClass", 1 + startPosition, endPosition)._2
    }

  private val chars = Seq(' ', '.')

  def complete(code: String, position: Int): List[CompletionModel] = compilerPool.use { compiler =>
    val incompleteClass =
      s"""$startClass
         |${code.substring(0, position)}""".stripMargin
    val bracketsDiff = Math.max(incompleteClass.count(_.equals('{')) - code.count(_.equals('}')), 0)

    val completeClass =
      s"""$incompleteClass
         |${(0 to bracketsDiff).map(s => endClass).reduce(_ + _)}""".stripMargin

    val lastCharIncomplete = incompleteClass.length
    val output = if (chars.contains(incompleteClass(lastCharIncomplete - 1))) {
      compiler.typeCompletion(completeClass, lastCharIncomplete)
    } else {
      val lastW = incompleteClass.substring(0, lastCharIncomplete).split("\\s|\\.").last
      val output =
        if (incompleteClass(lastCharIncomplete - lastW.length - 1).equals('.'))
          compiler.typeCompletion(completeClass, lastCharIncomplete - lastW.length - 1)
        else
          compiler.scopeCompletion(s"$completeClass", 1, lastCharIncomplete)._1 :::
            compiler.typeCompletion(completeClass, lastCharIncomplete + 1)

      output.filter(_.toComplete.startsWith(lastW)).distinct

    }.sortBy(_.toComplete)

    output

  }

  override def close(): Unit = compilerPool.close()
}
