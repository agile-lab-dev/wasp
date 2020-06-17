package it.agilelab.bigdata.wasp.compiler.utils

case class CompletionModel(toComplete : String,info : String ) {
  override def toString(): String = s"$toComplete : $info"
}
