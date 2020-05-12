package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.datastores.TopicCategory
import it.agilelab.bigdata.wasp.core.models._
import org.apache.commons.lang3.SerializationUtils
import org.mongodb.scala.bson.BsonObjectId

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

//TODO Spostarlo nella cartella test
class AllBLsTestWrapper {

  val batchJobBL: BatchJobBL = new BatchJobBL {
    val database = new ListBuffer[BatchJobModel]

    override def update(batchJobModel: BatchJobModel): Unit = {
      val index = database.indexWhere(b => b.name == (batchJobModel.name))
      database.update(index, batchJobModel)
    }

    override def getByName(name: String): Option[BatchJobModel] = {
      database.find(p => p.name == name)
    }



    override def getAll: Seq[BatchJobModel] = {
      database.toList
    }


    override def deleteByName(name: String): Unit = {
      val index = database.indexWhere(b => b.name == name)
      database.remove(index)
    }

    override def insert(batchJobModel: BatchJobModel): Unit = database :+ batchJobModel

    override def instances(): BatchJobInstanceBL = new BatchJobInstanceBL {

      override def all(): Seq[BatchJobInstanceModel] = ???

      override def instancesOf(name: String): Seq[BatchJobInstanceModel] = ???

      override def update(instance: BatchJobInstanceModel): BatchJobInstanceModel = ???

      override def insert(instance: BatchJobInstanceModel): BatchJobInstanceModel = ???

      override def getByName(name: String): Option[BatchJobInstanceModel] = ???
    }
  }

  val indexBL: IndexBL = new IndexBL {
    val database = new ListBuffer[IndexModel]

    override def getByName(name: String): Option[IndexModel] = ???

    override def persist(indexModel: IndexModel): Unit = {
      database.+=(indexModel)
    }

    override def getAll() = database
  }

  val mlModelBL: MlModelBL = new MlModelBL {
    val database = new ListBuffer[MlModelOnlyInfo]
    val fs = new mutable.HashMap[String, Array[Byte]]()


    override def saveMlModelOnlyInfo(mlModelOnlyInfo: MlModelOnlyInfo): Unit = {
      database.+=:(mlModelOnlyInfo)
    }

    override def getSerializedTransformer(mlModelOnlyInfo: MlModelOnlyInfo): Option[Any] = {
      val arrayByte = fs.get(mlModelOnlyInfo.modelFileId.get.toString()).get
      val obj: Any = SerializationUtils.deserialize(arrayByte)
      Some(obj)
    }

    override def saveTransformer(transformerModel: Serializable, name: String, version: String, timestamp: Long): BsonObjectId = {
      val arrayByte = SerializationUtils.serialize(transformerModel)
      val key = BsonObjectId()
      fs.put(key.toString(), arrayByte)
      key
    }

    override def getMlModelOnlyInfo(name: String, version: String): Option[MlModelOnlyInfo] = {
      val model = database.filter(p => p.name == name && p.version == version).maxBy(_.timestamp.getOrElse(0l))

     Some(model)
    }

    override def getMlModelOnlyInfo(name: String, version: String, timestamp: Long): Option[MlModelOnlyInfo] = {
      val model = database.filter(p => p.name == name && p.version == version && p.timestamp.get == timestamp)
      model.headOption
    }

    override def getAll: Seq[MlModelOnlyInfo] = database

    /**
     * Delete the metadata and the transformer model in base to name, version, timestamp
     * @param name
     * @param version
     * @param timestamp
     * @return
     */
    override def delete(name: String, version: String, timestamp: Long): Unit = ???

    /**
     * Update only the metadata about the model
     * @param mlModelOnlyInfo
     * @return
     */
    override def updateMlModelOnlyInfo(mlModelOnlyInfo: MlModelOnlyInfo): Unit = ???
  }

  val topicBL = new TopicBL {
    val database = new ListBuffer[DatastoreModel[TopicCategory]]
    override def getByName(name: String): Option[DatastoreModel[TopicCategory]] = database.find(_.name == name)
    
    override def persist(topicModel: DatastoreModel[TopicCategory]): Unit = {
      database.+=(topicModel)
    }

    override def getAll: Seq[DatastoreModel[TopicCategory]] = database
  }

  val producerBL = new ProducerBL {
    val database = new ListBuffer[ProducerModel]

    override def update(producerModel: ProducerModel): Unit = {
      val index = database.indexWhere(_.name == producerModel.name)
      database.remove(index)
      database += producerModel
    }

    override def getByName(name: String): Option[ProducerModel] = database.find(_.name == name)

    override def getActiveProducers(isActive: Boolean): Seq[ProducerModel] = database.filter(_.isActive == isActive)
  
    override def getSystemProducers: Seq[ProducerModel] = database.filter(_.isSystem == true)
  
    override def getNonSystemProducers: Seq[ProducerModel] = database.filter(_.isSystem == false)
  
    override def getAll: Seq[ProducerModel] = database.toList

    override def getTopic(topicBL: TopicBL, producerModel: ProducerModel): Option[TopicModel] = {
      if (producerModel.hasOutput)
        topicBL.getTopicModelByName(producerModel.topicName.get)
      else
        None
    }

    override def persist(producerModel: ProducerModel):  Unit = {
      database.+=(producerModel)
    }

    override def getByTopicName(name: String): Seq[ProducerModel] = {
      database.filter(_.topicName == name)
    }
  }
  
  // TODO implement this
  val rawBL = new RawBL {
    override def getByName(name: String): Option[RawModel] = ???

    override def persist(rawModel: RawModel): Unit = ???
  }

  val keyValueBL = new KeyValueBL {override def getByName(name: String) = ???

    override def persist(rawModel: KeyValueModel) = ???

    override def getAll(): Seq[KeyValueModel] = ???
  }
}

