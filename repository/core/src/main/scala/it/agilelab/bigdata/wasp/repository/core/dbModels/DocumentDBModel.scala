package it.agilelab.bigdata.wasp.repository.core.dbModels

import it.agilelab.bigdata.wasp.models.Model

trait DocumentDBModel extends Model

case class DocumentDBModelV1(override val name: String,
                             connectionString: String,
                             schema: String
                            ) extends DocumentDBModel

