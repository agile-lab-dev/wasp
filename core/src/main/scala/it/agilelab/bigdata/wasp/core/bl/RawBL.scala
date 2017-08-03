package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.{RawModel}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.bson.BsonString
import org.mongodb.scala.bson.BsonObjectId



trait RawBL {
	
	def getByName(name: String): Option[RawModel]
	
	def getById(id: String): Option[RawModel]
	
	def persist(rawModel: RawModel): Unit
}

class RawBLImp(waspDB: WaspDB) extends RawBL  {
	
	private def factory(t: RawModel) = new RawModel(t.name, t.uri, t.timed, t.schema, t.options, t._id)
	
	def getByName(name: String) = {
		waspDB.getDocumentByField[RawModel]("name", new BsonString(name)).map(factory)
	}
	
	def getById(id: String) = {
		waspDB.getDocumentByID[RawModel](BsonObjectId(id)).map(factory)
	}
	
	override def persist(rawModel: RawModel): Unit = waspDB.insert[RawModel](rawModel)
}