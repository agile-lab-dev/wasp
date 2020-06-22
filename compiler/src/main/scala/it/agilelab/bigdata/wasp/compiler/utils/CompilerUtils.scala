package it.agilelab.bigdata.wasp.compiler.utils

object CompilerUtils {

  private val startClass = """object Compilation {""".stripMargin
  private val endClass = """}""".stripMargin
  val compiler = Compiler

  def validate(code : String,startPosition:  Int=0): List[ErrorModel] = compiler.synchronized {
    val completeClass =
      s"""$startClass
         |$code
         |$endClass""".stripMargin
    val endPosition = completeClass.length

    compiler.scopeCompletion(s"$completeClass",1+startPosition,endPosition)

  }

  private val chars = Seq(' ','.')

  def complete(code : String, position : Int): List[CompletionModel] = compiler.synchronized {


    val incompleteClass =
      s"""$startClass
         |${code.substring(0,position)}""".stripMargin
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
