package it.agilelab.bigdata.wasp.repository.core.dbModels

import it.agilelab.bigdata.wasp.models.{KeyValueOption, Model}

trait KeyValueDBModel extends Model

case class KeyValueDBModelV1(override val name: String,
                             tableCatalog: String,
                             dataFrameSchema: Option[String],
                             options: Option[Seq[KeyValueOption]],
                             useAvroSchemaManager: Boolean,
                             avroSchemas: Option[Map[String, String]]
                            ) extends KeyValueDBModel