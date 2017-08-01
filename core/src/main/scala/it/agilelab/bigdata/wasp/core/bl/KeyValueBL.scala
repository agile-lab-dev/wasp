package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.{BSONConversionHelper, KeyValueModel, RawModel}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import reactivemongo.bson.{BSONObjectID, BSONString}
import reactivemongo.api.commands.WriteResult

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global


trait KeyValueBL {

	def getByName(name: String): Future[Option[KeyValueModel]]

	def getById(id: String): Future[Option[KeyValueModel]]

	def persist(rawModel: KeyValueModel): Future[WriteResult]
}

class KeyValueBLImp(waspDB: WaspDB) extends KeyValueBL with BSONConversionHelper {
	
	private def factory(t: KeyValueModel) = new KeyValueModel(t.name, t.schema, t.dataFrameSchema, t.avroSchemas)
	
	def getByName(name: String) = {
		waspDB.getDocumentByField[KeyValueModel]("name", new BSONString(name)).map(index => {
			index.map(p => factory(p))
		})
	}
	
	def getById(id: String) = {
		waspDB.getDocumentByID[KeyValueModel](BSONObjectID(id)).map(Index => {
			Index.map(p => factory(p))
		})
	}
	
	override def persist(rawModel: KeyValueModel): Future[WriteResult] = waspDB.insert[KeyValueModel](rawModel)
}