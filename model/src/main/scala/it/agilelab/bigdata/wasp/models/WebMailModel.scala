package it.agilelab.bigdata.wasp.models

import it.agilelab.bigdata.wasp.datastores.WebMailCategory

case class WebMailModel (override val name: String)
  extends DatastoreModel[WebMailCategory]