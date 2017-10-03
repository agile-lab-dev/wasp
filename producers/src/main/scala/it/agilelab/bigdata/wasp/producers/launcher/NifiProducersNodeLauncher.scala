package it.agilelab.bigdata.wasp.producers.launcher

import java.io.File

import it.agilelab.bigdata.wasp.core.models.{MlModelOnlyInfo, ProducerModel}
import it.agilelab.bigdata.wasp.producers.NifiProducerModel
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.SerializationUtils
import org.bson.BsonDocument

/**
  * Launcher for the NifiProducer.
  *
  * @author Alessandro Marino
  */

case class modelFile(data: Array[Byte], fileName: String, metadata: BsonDocument)

object NifiProducersNodeLauncher extends ProducersNodeLauncherTrait {
  override def launch(args: Array[String]): Unit = {
    super.launch(args)
    waspDB.insertIfNotExists[ProducerModel](NifiProducerModel.nifiProducer)
  }
}

object InsertModelLauncher extends ProducersNodeLauncherTrait {
  override def launch(args: Array[String]): Unit = {
    super.launch(args)
    val fileName = "/home/amarino/Agile.Wasp2/RegressionSumModels_1"

    val data = new File(fileName)
    val vec: Array[Byte] = FileUtils.readFileToByteArray(data)
    val res = waspDB.saveFile(vec, fileName, new BsonDocument())
  }

}