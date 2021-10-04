package it.agilelab.bigdata.wasp.repository.core.dbModels

import it.agilelab.bigdata.wasp.models.Model
import org.mongodb.scala.bson.BsonDocument

trait ProcessGroupDBModel extends Model

case class ProcessGroupDBModelV1(name: String,
                                 content: BsonDocument,
                                 errorPort: String
                                ) extends ProcessGroupDBModel
