package it.agilelab.bigdata.wasp.core.models.configuration

import it.agilelab.bigdata.wasp.core.models.Model

case class NifiConfigModel(
    nifiBaseUrl: String,
    nifiApiPath: String,
    nifiUiPath: String,
    name: String
) extends Model
