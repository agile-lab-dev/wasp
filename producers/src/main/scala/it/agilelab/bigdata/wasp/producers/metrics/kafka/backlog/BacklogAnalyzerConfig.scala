package it.agilelab.bigdata.wasp.producers.metrics.kafka.backlog

import com.typesafe.config.{Config, ConfigRenderOptions, ConfigValueType}
import it.agilelab.bigdata.wasp.core.datastores.DatastoreProduct.KafkaProduct
import it.agilelab.bigdata.wasp.core.models.{PipegraphModel, StructuredStreamingETLModel}


case class BacklogAnalyzerConfig(pipegraph: PipegraphModel,
                                 etls: List[StructuredStreamingETLModel])

object BacklogAnalyzerConfig {
  private val PIPEGRAPH_NAME = "pipegraphName"

  def fromConfig(conf: Config, pipegraphs: Map[String, PipegraphModel]): Either[String, BacklogAnalyzerConfig] = {
    def pName(c: Config): Either[String, String] = {
      if (c.hasPath(PIPEGRAPH_NAME) &&
        c.getValue(PIPEGRAPH_NAME).valueType() == ConfigValueType.STRING) {
        Right(c.getString(PIPEGRAPH_NAME))
      } else {
        Left(s"Cannot retrieve pipegraphName from conf: ${c.root().render(ConfigRenderOptions.concise())}")
      }
    }

    def pModel(name: String, pipegraphs: Map[String, PipegraphModel]): Either[String, PipegraphModel] = {
      pipegraphs.get(name).toRight(s"Cannot find pipegraph name $name")
    }

    def eModels(pipegraphModel: PipegraphModel): Either[String, List[StructuredStreamingETLModel]] = {
      val (kafkaInput, notKafkaInput) = pipegraphModel.structuredStreamingComponents.partition { etlModel =>
        etlModel.streamingInput.datastoreProduct == KafkaProduct
      }
      kafkaInput match {
        case Nil => Left(s"Pipegraph ${pipegraphModel.name} did not contain any etl which reads from Kafka, " +
          s"instead it contained the following etls:\n\t${notKafkaInput.mkString("\n\t")}")
        case etls => Right(etls)
      }
    }

    for {
      pipegraphName <- pName(conf).right
      pipegraphModel <- pModel(pipegraphName, pipegraphs).right
      etlModels <- eModels(pipegraphModel).right
    } yield {
      BacklogAnalyzerConfig(pipegraphModel, etlModels)
    }
  }
}