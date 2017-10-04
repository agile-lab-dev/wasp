package it.agilelab.bigdata.wasp.producers.launcher

import java.io.File

import it.agilelab.bigdata.wasp.core.models.{MlModelOnlyInfo, ProducerModel}
import it.agilelab.bigdata.wasp.producers.NifiProducerModel
import org.apache.commons.io.FileUtils
import org.bson.BsonDocument

/**
  * Launcher for the NifiProducer.
  *
  * @author Alessandro Marino
  */

object NifiProducersNodeLauncher extends ProducersNodeLauncherTrait {
  override def launch(args: Array[String]): Unit = {
    super.launch(args)
    waspDB.insertIfNotExists[ProducerModel](NifiProducerModel.nifiProducer)
  }
}

object InsertModelLauncher extends ProducersNodeLauncherTrait {
  override def launch(args: Array[String]): Unit = {
    super.launch(args)
    val path = "/home/amarino/Agile.Wasp2/"
    val fileName = "RegressionSumModels_1"

    val data = new File(path + fileName)
    val vec: Array[Byte] = FileUtils.readFileToByteArray(data)
    val modelField = waspDB.saveFile(vec, path + fileName, new BsonDocument())
    val mlModel = MlModelOnlyInfo(fileName, "", None, None, Some(modelField), favorite = false, "", None)
    waspDB.insertIfNotExists[MlModelOnlyInfo](mlModel)
  }
}