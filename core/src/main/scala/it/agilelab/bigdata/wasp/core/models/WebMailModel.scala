package it.agilelab.bigdata.wasp.core.models

import it.agilelab.bigdata.wasp.core.datastores.WebMailCategory

case class WebMailModel (override val name: String)
  extends DatastoreModel[WebMailCategory]