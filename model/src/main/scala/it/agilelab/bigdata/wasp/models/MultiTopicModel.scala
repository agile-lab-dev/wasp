package it.agilelab.bigdata.wasp.models

import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.KafkaProduct

/**
	* A model for grouping of topics.
	*
	* The `name` field specifies the name of the model, which is used as the unique identifier for the model in the
	* models database.
	*
	* The `topicNameField` field specifies the field whose contents will be used as the name of the topic to which the
	* message will be sent when writing to Kafka. The field must be of type string. The original field will be left as-is,
	* so your schema must handle it (or you can use `valueFieldsNames`).
	*
	* The `topicModelNames` contains the names of the topic model that constitute this grouping of topics.
	*
	* The topic models that constitute this grouping of topics must:
	* - consist of at least one topic model
	* - be all different models
	* - refer to different topics
	* - use the same settings for everything but partitions and replicas
	*/
case class MultiTopicModel private[wasp] (
    override val name: String,
    topicNameField: String,
    topicModelNames: Seq[String]
) extends DatastoreModel {
  override def datastoreProduct: DatastoreProduct = KafkaProduct
}

object MultiTopicModel {

  type TopicCompressionValidationError = Map[TopicCompression, Seq[TopicModel]]

  def fromTopicModels(name: String, topicNameField: String, topicModels: Seq[TopicModel]): MultiTopicModel = {
    areTopicsHealthy(topicModels).fold(
      s => throw new IllegalArgumentException(s),
      _ => ()
    )
    new MultiTopicModel(name, topicNameField, topicModels.map(_.name))
  }

  /**
    * Checks that:
    * - there is at least one topic model
    * - the topic models are all different models
    * - the topic models refer to different topics
    * - the topic models have the same compression
    */
  @com.github.ghik.silencer.silent("Unused import")
  private[wasp] def areTopicsHealthy(models: Seq[TopicModel]): Either[String, Unit] = {
    import it.agilelab.bigdata.wasp.utils.EitherUtils._
    for {
      _ <- Either.cond(models.nonEmpty, (), "There must be at least one topic model")
      _ <- Either.cond(models.size == models.distinct.size, (), "Each topic model can only appear once")
      _ <- Either.cond(models.size == models.map(_.name).distinct.size, (), "Each topic can only appear once")
      r <- validateTopicModelsHaveSameCompression(models).left.map(formatTopicCompressionValidationError)
    } yield r
  }

  def validateTopicModelsHaveSameCompression(topics: Seq[TopicModel]): Either[TopicCompressionValidationError, Unit] = {
    val grouped = topics.groupBy(_.topicCompression)
    if (grouped.keySet.size != 1) {
      Left(grouped)
    } else {
      Right(())
    }
  }

  private[wasp] def formatTopicCompressionValidationError(error: TopicCompressionValidationError): String = {
    error
      .map {
        case (compression, topics) =>
          val t = TopicCompression.asString(compression)
          topics.map(_.name).mkString(s"[", ",", s"] use $t")
      }
      .mkString("All topic models must have the same compression setting, found settings: ", ",", "")
  }
}
