package it.agilelab.bigdata.wasp.models

case class ErrorModel(fileName: String, where : String,errorType : String, msg :String,content:String,indicator :String) {
  override def toString() : String = {f"$fileName:$where: $errorType:$msg%n$content%n$indicator"}
}
