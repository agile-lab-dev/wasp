package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.RawModel



trait RawBL {
	
	def getByName(name: String): Option[RawModel]

	def persist(rawModel: RawModel): Unit

	def upsert(rawModel: RawModel): Unit

	def getAll() : Seq[RawModel]
}

