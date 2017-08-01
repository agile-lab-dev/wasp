package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.{BSONConversionHelper, RawModel}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import reactivemongo.bson.{BSONObjectID, BSONString}
import reactivemongo.api.commands.WriteResult

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


trait RawBL {
	
	def getByName(name: String): Future[Option[RawModel]]
	
	def getById(id: String): Future[Option[RawModel]]
	
	def persist(rawModel: RawModel): Future[WriteResult]
}

class RawBLImp(waspDB: WaspDB) extends RawBL with BSONConversionHelper {
	
	private def factory(t: RawModel) = new RawModel(t.name, t.uri, t.timed, t.schema, t.options, t._id)
	
	def getByName(name: String) = {
		waspDB.getDocumentByField[RawModel]("name", new BSONString(name)).map(index => {
			index.map(p => factory(p))
		})
	}
	
	def getById(id: String) = {
		waspDB.getDocumentByID[RawModel](BSONObjectID(id)).map(Index => {
			Index.map(p => factory(p))
		})
	}
	
	override def persist(rawModel: RawModel): Future[WriteResult] = waspDB.insert[RawModel](rawModel)
}