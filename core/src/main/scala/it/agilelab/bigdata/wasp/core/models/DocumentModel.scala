package it.agilelab.bigdata.wasp.core.models

import it.agilelab.bigdata.wasp.core.datastores.DocumentCategory

case class DocumentModel(override val name: String, connectionString: String, schema: String) extends DatastoreModel[DocumentCategory]
