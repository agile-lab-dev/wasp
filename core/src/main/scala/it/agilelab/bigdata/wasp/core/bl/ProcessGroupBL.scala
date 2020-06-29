package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.ProcessGroupModel
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.mongodb.scala.bson.BsonString

trait ProcessGroupBL {

  def getById(pgId: String): Seq[ProcessGroupModel]

  def insert(versionedProcessGroup: ProcessGroupModel): Unit

}

class ProcessGroupBLImpl(waspDB: WaspDB) extends ProcessGroupBL {

  override def getById(pgId: String): Seq[ProcessGroupModel] =
    waspDB
      .getAllDocumentsByField[ProcessGroupModel]("name", BsonString(pgId))

  override def insert(processGroup: ProcessGroupModel): Unit =
    waspDB.insert[ProcessGroupModel](processGroup)

}
