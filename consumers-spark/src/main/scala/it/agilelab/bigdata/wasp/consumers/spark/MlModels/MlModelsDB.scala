package it.agilelab.bigdata.wasp.consumers.spark.MlModels

import it.agilelab.bigdata.wasp.repository.core.bl.MlModelBL
import it.agilelab.bigdata.wasp.models.MlModelOnlyInfo
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.Params
import org.mongodb.scala.bson.BsonObjectId

import scala.collection.mutable.ListBuffer




/**
 * A wrapper of MlModelBl to use the the spark-ml classes
 */
class MlModelsDB(env: {val mlModelBL: MlModelBL})  {


  /**
   * Find the most recent TransformerWithInfo
   * @param name
   * @param version
   * @return
   */
  @throws[Exception]
  def read(name: String, version: String): TransformerWithInfo = {
    read(name, version, None)
  }

  /**
   * Get the TransformerWithInfo of only metadata
   * @param mlModelOnlyInfo
   * @return
   */
  @throws[Exception]
  def read(mlModelOnlyInfo: MlModelOnlyInfo): TransformerWithInfo = {
    read(mlModelOnlyInfo.name, mlModelOnlyInfo.version, mlModelOnlyInfo.timestamp)
  }

  /**
   * This method can be use to retrieve a complete model with own info,
   * if there isn't a timestamp it will take the most recent
   * @param name
   * @param version
   * @param timestampOpt
   * @return
   */
  @throws[Exception]
  def read(name: String, version: String, timestampOpt: Option[Long]): TransformerWithInfo = {
    val transformerInfoOption: Option[MlModelOnlyInfo] = readOnlyInfo(name, version, timestampOpt)
  
    transformerInfoOption match {
      case Some(transformerInfo) => {
        env.mlModelBL.getSerializedTransformer(transformerInfo) match {
           case Some(transformerAny) => {
             //Metto insieme le info del modello con il modello stesso
             val transfomer = transformerAny.asInstanceOf[Transformer with Params]

             TransformerWithInfo.create(transformerInfo, transfomer)
           }
           // Da gestire l'eccezione nel caso di mancanza del file
           case None => throw new Exception(s"The file of transformaer model not found: name: $name, version: $version, timestamp: $timestampOpt")
         }
      }

      // Da gestire l'eccezione nel caso di mancanza delle info
      case None => throw new Exception(s"The model info not found: name: $name, version: $version, timestamp: $timestampOpt")
    }
  }

  /**
   * It build a MlModelsBroadcastDB where all the model with info are broadcasted
   * The param can be incomplete so it read the complete model with info from MongoDB
   * @param mlModelsOnlyInfo
   * @param sc
   * @return
   */
  def createModelsBroadcast(mlModelsOnlyInfo: List[MlModelOnlyInfo])(implicit sc: SparkContext): MlModelsBroadcastDB = {
    // read all models and put them in broadcast dbs
    val mlModelsList: List[MlModelsBroadcastDB] = mlModelsOnlyInfo.map(model => MlModelsBroadcastDB(read(model)))
    
    // combine ml model broadcast dbs
    mlModelsList.fold(MlModelsBroadcastDB())(_ + _)
  }

  /**
   * It save the info on the own collection and the transformer model in gridFS
   * @param mlModel
   * @return an error if something was wrong and the saved model with all the ids initialized
   */
  def write(mlModel: TransformerWithInfo): TransformerWithInfo = {
    val fileId: BsonObjectId = env.mlModelBL.saveTransformer(mlModel.transformer, mlModel.name, mlModel.version, mlModel.timestamp)
    env.mlModelBL.saveMlModelOnlyInfo(mlModel.toOnlyInfo(fileId))
    
    mlModel.copy(modelFileId = Some(fileId))
  }

  def write(mlModels: List[TransformerWithInfo]): List[TransformerWithInfo] = {
    mlModels.map(write)
  }

  def delete(name: String, version: String, timestamp: Long): Unit = {
    env.mlModelBL.delete(name, version, timestamp)
  }

  def readOnlyInfo(name: String, version: String, timestampOpt: Option[Long]): Option[MlModelOnlyInfo] = {
    timestampOpt match {
      case Some(timestamp) => env.mlModelBL.getMlModelOnlyInfo(name, version, timestamp)
      case None => env.mlModelBL.getMlModelOnlyInfo(name, version)
    }

  }
}



/**
 * It allow to get a complete broadcasted model and keep a list of model to salve
 */
class MlModelsBroadcastDB(val modelDatabase: Map[String, Broadcast[TransformerWithInfo]] = Map(),
                           val timestampMap: Map[String, Long] = Map())
  extends Serializable {

  /**
   * List of models to salve
   */
  private val modelsToSalve = new ListBuffer[TransformerWithInfo]()

  /**
   * Merge with another MlModelsBroadcastDB
   * @param other
   * @return
   */
  def +(other: MlModelsBroadcastDB): MlModelsBroadcastDB = {
    val modelDatabaseResult = modelDatabase ++ other.modelDatabase

    // Calcolo il timestamp più recente
    val timestampMapResult: Map[String, Long] = (timestampMap.seq ++ other.timestampMap.seq).groupBy(_._1).map {
      case (timestampKey: String, timestamps: Map[String, Long]) =>
        (timestampKey, timestamps.values.max)
    }
    new MlModelsBroadcastDB(modelDatabaseResult, timestampMapResult)
  }

  /**
   * Return a complete broadcasted model
   * @param name
   * @param version
   * @param timestampOpt
   * @return
   */
  def getBroadcast(name: String, version: String, timestampOpt: Option[Long] = None): Option[Broadcast[TransformerWithInfo]] = {
    val timestamp: Option[Long] = timestampOpt match {
      case None => timestampMap.get(MlModelsBroadcastDB.getPartialKey(name, version))
      case p => p
    }

    timestamp.flatMap(t => {
      val key = MlModelsBroadcastDB.getKey(name, version, t)
      modelDatabase.get(key)
    })
  }

  /**
   * Return a model in base the three key name, version, timestamp(optional if None will choose the most recent)
   * @param name
   * @param version
   * @param timestampOpt
   * @return
   */
  def get(name: String, version: String, timestampOpt: Option[Long] = None): Option[TransformerWithInfo] = {
    getBroadcast(name, version, timestampOpt).map(_.value)
  }

  def addModelToSave(completeModel: TransformerWithInfo): Unit = modelsToSalve.+=(completeModel)
  def getModelsToSave = modelsToSalve.toList
}

object MlModelsBroadcastDB {
  val empty = new MlModelsBroadcastDB()
  def apply() = empty

  /**
   * Create a MlModelsBroadcastDB from a model, it will be broadcast
   * @param transformer
   * @param sc
   * @return
   */
  def apply(transformer: TransformerWithInfo)(implicit sc: SparkContext): MlModelsBroadcastDB = {
    val key = getKey(name = transformer.name, version = transformer.version, timestamp = transformer.timestamp)
    val partialKey = getPartialKey(name = transformer.name, version = transformer.version)

    val broadcastedTransformer = sc.broadcast(transformer)
    val modelEntry = key -> broadcastedTransformer
    val timestampEntry = partialKey -> transformer.timestamp

    new MlModelsBroadcastDB(Map(modelEntry), Map(timestampEntry))
  }

  /**
   * The unique main key
   * @param name
   * @param version
   * @param timestamp
   * @return
   */
  private def getKey(name: String, version: String, timestamp: Long) = s"$name-$version-$timestamp"

  /**
   * The partial key that identify a list of model with the same name, version
   * @param name
   * @param version
   * @return
   */
  private def getPartialKey(name: String, version: String) = s"$name-$version"
}