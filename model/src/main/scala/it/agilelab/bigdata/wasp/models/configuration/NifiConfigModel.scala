package it.agilelab.bigdata.wasp.models.configuration

import it.agilelab.bigdata.wasp.models.Model

case class NifiConfigModel(
    nifiBaseUrl: String,
    nifiApiPath: String,
    nifiUiPath: String,
    name: String
) extends Model
