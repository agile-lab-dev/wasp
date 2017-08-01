package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models._
import org.apache.commons.lang3.SerializationUtils
import play.api.libs.iteratee.Enumerator
import reactivemongo.bson.BSONObjectID
import reactivemongo.api.commands._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

//TODO Spostarlo nella cartella test
class AllBLsTestWrapper {
  val success = Future.successful(DefaultWriteResult(ok = true, n = 0, writeErrors = Nil, writeConcernError = None,
    code = None,
    errmsg = None))

  val batchJobBL: BatchJobBL = new BatchJobBL {
    val database = new ListBuffer[BatchJobModel]

    override def update(batchJobModel: BatchJobModel): Future[WriteResult] = {
      val index = database.indexWhere(b => b._id == batchJobModel._id)
      database.update(index, batchJobModel)
      success
    }

    override def getById(id: String): Future[Option[BatchJobModel]] = {
      Future(database.find(p => p._id.get.stringify == id))
    }

    override def getPendingJobs(state: String): Future[List[BatchJobModel]] = {
      Future(database.filter(p => p.state == state).toList)
    }

    override def getAll: Future[List[BatchJobModel]] = {
      Future(database.toList)
    }

    override def persist(batchJobModel: BatchJobModel): Future[WriteResult] = {
      database.+=(batchJobModel)
      success
    }

    override def insert(batchJobModel: BatchJobModel): Future[WriteResult] = persist(batchJobModel)

    override def deleteById(id_string: String): Future[WriteResult] = {
      val index = database.filter(_._id.isDefined).indexWhere(b => b._id.get.stringify == id_string)
      database.remove(index)
      success
    }
  }

  val indexBL: IndexBL = new IndexBL {
    val database = new ListBuffer[IndexModel]

    override def getByName(name: String): Future[Option[IndexModel]] = ???

    override def getById(id: String): Future[Option[IndexModel]] =Future(database.find(_._id.get.stringify == id))

    override def persist(indexModel: IndexModel): Future[WriteResult] = {
      database.+=(indexModel)
      success
    }
  }

  val mlModelBL: MlModelBL = new MlModelBL {
    val database = new ListBuffer[MlModelOnlyInfo]
    val fs = new mutable.HashMap[String, Array[Byte]]()


    override def saveMlModelOnlyInfo(mlModelOnlyInfo: MlModelOnlyInfo): Future[WriteResult] = {
      database.+=:(mlModelOnlyInfo)
      success
    }

    override def getSerializedTransformer(mlModelOnlyInfo: MlModelOnlyInfo): Future[Option[Enumerator[Any]]] = {
      val arrayByte = fs.get(mlModelOnlyInfo.modelFileId.get.toString()).get
      val obj: Any = SerializationUtils.deserialize(arrayByte)
      Future.successful(Some(Enumerator(obj)))
    }

    override def saveTransformer(transformerModel: Serializable, name: String, version: String, timestamp: Long): Future[BSONObjectID] = {
      val arrayByte = SerializationUtils.serialize(transformerModel)
      val key = BSONObjectID.generate
      fs.put(key.toString(), arrayByte)
      Future.successful(key)
    }

    override def getMlModelOnlyInfo(name: String, version: String): Future[Option[MlModelOnlyInfo]] = {
      val model = database.filter(p => p.name == name && p.version == version).maxBy(_.timestamp.getOrElse(0l))

      Future.successful(Some(model))
    }

    override def getMlModelOnlyInfo(name: String, version: String, timestamp: Long): Future[Option[MlModelOnlyInfo]] = {
      val model = database.filter(p => p.name == name && p.version == version && p.timestamp.get == timestamp)
      Future.successful(model.headOption)
    }

    override def getAll: Future[List[MlModelOnlyInfo]] = Future.successful(database.toList)

    override def getById(id: String): Future[Option[MlModelOnlyInfo]] = ???

    override def delete(id: String): Future[Option[WriteResult]] = ???

    /**
     * Delete the metadata and the transformer model in base to name, version, timestamp
     * @param name
     * @param version
     * @param timestamp
     * @return
     */
    override def delete(name: String, version: String, timestamp: Long): Future[Option[WriteResult]] = ???

    /**
     * Update only the metadata about the model
     * @param mlModelOnlyInfo
     * @return
     */
    override def updateMlModelOnlyInfo(mlModelOnlyInfo: MlModelOnlyInfo): Future[WriteResult] = ???
  }

  val topicBL = new TopicBL {
    val database = new ListBuffer[TopicModel]
    override def getByName(name: String): Future[Option[TopicModel]] = Future(database.find(_.name == name))

    override def getById(id: String): Future[Option[TopicModel]] = Future(database.find(_._id.get.stringify == id))

    override def persist(topicModel: TopicModel): Future[WriteResult] = {
      database.+=(topicModel)
      success
    }

    override def getAll: Future[List[TopicModel]] = Future(database.toList)
  }

  val producerBL = new ProducerBL {
    val database = new ListBuffer[ProducerModel]

    override def update(producerModel: ProducerModel): Future[WriteResult] = {
      val index = database.indexWhere(_.name == producerModel.name)
      database.remove(index)
      database += producerModel
      success
    }

    override def getByName(name: String): Future[Option[ProducerModel]] = Future(database.find(_.name == name))

    override def getByTopicId(id_topic: BSONObjectID): Future[List[ProducerModel]] = Future(database.filter(_.id_topic.isDefined).filter(_.id_topic.get == id_topic).toList)

    override def getById(id: String): Future[Option[ProducerModel]] = Future(database.find(p => p._id.isDefined && p._id.get.stringify == id))

    override def getActiveProducers(isActive: Boolean): Future[List[ProducerModel]] = Future(database.filter(_.isActive == isActive).toList)

    override def getAll: Future[List[ProducerModel]] = Future(database.toList)

    override def getTopic(topicBL: TopicBL, producerModel: ProducerModel): Future[Option[TopicModel]] = {
      if (producerModel.hasOutput)
        topicBL.getById(producerModel.id_topic.get.stringify)
      else
        Future(None)
    }

    override def persist(producerModel: ProducerModel): Future[WriteResult] = {
      database.+=(producerModel)
      success
    }

  }
  
  // TODO implement this
  val rawBL = new RawBL {
    override def getByName(name: String): Future[Option[RawModel]] = ???
  
    override def getById(id: String): Future[Option[RawModel]] = ???
  
    override def persist(rawModel: RawModel): Future[LastError] = ???
  }

  val keyValueBL = new KeyValueBL {override def getByName(name: String) = ???

    override def persist(rawModel: KeyValueModel) = ???

    override def getById(id: String) = ???
  }
}

