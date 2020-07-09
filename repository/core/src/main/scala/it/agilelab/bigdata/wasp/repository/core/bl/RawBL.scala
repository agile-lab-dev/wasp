package it.agilelab.bigdata.wasp.repository.core.bl

import it.agilelab.bigdata.wasp.models.RawModel


trait RawBL {
	
	def getByName(name: String): Option[RawModel]

	def persist(rawModel: RawModel): Unit

	def upsert(rawModel: RawModel): Unit

	def getAll() : Seq[RawModel]
}

