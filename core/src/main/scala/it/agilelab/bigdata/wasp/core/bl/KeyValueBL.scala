package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.KeyValueModel
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.bson.BsonString
import org.mongodb.scala.bson.BsonObjectId

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global


trait KeyValueBL {

	def getByName(name: String): Option[KeyValueModel]

	def getById(id: String): Option[KeyValueModel]

	def persist(rawModel: KeyValueModel): Unit
}

class KeyValueBLImp(waspDB: WaspDB) extends KeyValueBL {
	
	private def factory(t: KeyValueModel) = KeyValueModel(t.name, t.schema, t.dataFrameSchema, t.options, t.avroSchemas)
	
	def getByName(name: String) = {
		waspDB.getDocumentByField[KeyValueModel]("name", new BsonString(name)).map(index => {
			factory(index)
		})
	}
	
	def getById(id: String) = {
		waspDB.getDocumentByID[KeyValueModel](BsonObjectId(id)).map(index => {
			factory(index)
		})
	}
	
	override def persist(rawModel: KeyValueModel): Unit = waspDB.insert[KeyValueModel](rawModel)
}