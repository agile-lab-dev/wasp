package it.agilelab.bigdata.wasp.models

case class CompletionModel(toComplete : String,info : String ) {
  override def toString(): String = s"$toComplete : $info"
}
