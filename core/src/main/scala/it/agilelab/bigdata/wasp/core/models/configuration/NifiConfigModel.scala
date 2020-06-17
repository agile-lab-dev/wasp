package it.agilelab.bigdata.wasp.core.models.configuration

import it.agilelab.bigdata.wasp.core.models.Model

case class NifiConfigModel(
    apiUrl: String,
    uiUrl: String,
    name: String
) extends Model
