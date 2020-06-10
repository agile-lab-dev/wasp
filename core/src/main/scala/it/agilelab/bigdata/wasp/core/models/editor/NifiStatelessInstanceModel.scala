package it.agilelab.bigdata.wasp.core.models.editor

import it.agilelab.bigdata.wasp.core.models.Model

case class NifiStatelessInstanceModel(
    name: String,
    url: String,
    flowId: String
) extends Model

case class CodeResponse(
    name: String,
    code: String
)
