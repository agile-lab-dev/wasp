package it.agilelab.bigdata.wasp.models

import it.agilelab.bigdata.wasp.datastores.DocumentCategory

case class DocumentModel(override val name: String, connectionString: String, schema: String) extends DatastoreModel[DocumentCategory]
