package it.agilelab.bigdata.wasp.repository.core.bl

import it.agilelab.bigdata.wasp.models.{GenericModel}


trait GenericBL {
	
	def getByName(name: String): Option[GenericModel]

	def persist(genericModel: GenericModel): Unit

	def upsert(genericModel: GenericModel): Unit

	def getAll() : Seq[GenericModel]
}

