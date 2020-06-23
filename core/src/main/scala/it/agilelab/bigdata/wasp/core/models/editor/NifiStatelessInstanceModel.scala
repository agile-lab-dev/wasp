package it.agilelab.bigdata.wasp.core.models.editor

import it.agilelab.bigdata.wasp.core.models.Model
import org.json4s.JsonAST.JObject

case class NifiStatelessInstanceModel(
    name: String,
    url: String,
    processGroupId: String
) extends Model

case class ProcessGroupResponse(
    id: String,
    contents: JObject
)
