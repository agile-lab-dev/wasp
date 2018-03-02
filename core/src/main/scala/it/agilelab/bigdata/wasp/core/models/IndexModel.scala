package it.agilelab.bigdata.wasp.core.models

import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.mongodb.scala.bson.{BsonDocument, BsonObjectId}

import scala.collection.JavaConverters._

object IndexModel {
  val readerType = "index"

}

case class IndexModel private[core] (override val name: String,
                      creationTime: Long,
                      schema: Option[String],
                      query: Option[String] = None,
                      numShards: Option[Int] = Some(1),
                      replicationFactor: Option[Int] = Some(1),
                      rollingIndex: Boolean = true,
                      idField: Option[String] = None)
  extends Model {

  def resource = s"$eventuallyTimedName/$dataType"

  def collection = eventuallyTimedName

  def eventuallyTimedName = if (rollingIndex) ConfigManager.buildTimedName(name) else name
  
  /**
    * Returns a JSON representation of the schema of this index's schema.
    * @return
    */
  def getJsonSchema: String = {
    schema.getOrElse("")
  }


  def dataType: String = name

}