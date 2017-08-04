package it.agilelab.bigdata.wasp.core.utils

import org.mongodb.scala.bson.BsonDocument

object JsonConverter {

    def toString(doc: BsonDocument): String = {
    doc.toJson()
  }

  def fromString(doc: String): Option[BsonDocument] = {
    try {
      Some(BsonDocument.apply(doc))
    }catch {
      case e: Exception => None
    }

  }


}