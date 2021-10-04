package it.agilelab.bigdata.wasp.repository.core.dbModels

import it.agilelab.bigdata.wasp.datastores.GenericProduct
import it.agilelab.bigdata.wasp.models.{GenericOptions, Model}
import org.mongodb.scala.bson.BsonDocument

trait GenericDBModel extends Model

case class GenericDBModelV1(override val name: String,
                            value: BsonDocument,
                            product: GenericProduct,
                            options: GenericOptions = GenericOptions.default
                           ) extends GenericDBModel
