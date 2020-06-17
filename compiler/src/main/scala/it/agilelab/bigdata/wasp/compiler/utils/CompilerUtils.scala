package it.agilelab.bigdata.wasp.compiler.utils

object CompilerUtils {

  private val startClass = """object Compilation {""".stripMargin
  private val endClass = """}""".stripMargin

  def validate(code : String,startPosition:  Int=0): List[ErrorModel] = {
    val compiler = new Compiler(1+startPosition)
    val completeClass =
      s"""$startClass
         |$code
         |$endClass""".stripMargin
    val position = completeClass.length

    compiler.scopeCompletion(s"$completeClass",position)

  }

  private val chars = Seq(' ','.')

  def complete(code : String): List[CompletionModel] = {

    val compiler = new Compiler(0)
    val incompleteClass =
      s"""$startClass
         |$code""".stripMargin
    val completeClass =
      s"""$incompleteClass
         |$endClass""".stripMargin



    val lastCharIncomplete = incompleteClass.length
    if(chars.contains(incompleteClass(lastCharIncomplete-1))) {
      compiler.typeCompletion(completeClass, lastCharIncomplete)
    }else {
      val lastW = incompleteClass.substring(0,lastCharIncomplete).split("\\s|\\.").last
      val output = if(incompleteClass(lastCharIncomplete-lastW.length-1).equals('.'))
        compiler.typeCompletion(completeClass, lastCharIncomplete - lastW.length - 1)
      else compiler.typeCompletion(completeClass, lastCharIncomplete + 1)

      output.filter(_.toComplete.startsWith(lastW))

    }.sortBy(_.toComplete)

  }



}
