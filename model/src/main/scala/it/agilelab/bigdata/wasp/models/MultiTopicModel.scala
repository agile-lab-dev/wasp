package it.agilelab.bigdata.wasp.models

import it.agilelab.bigdata.wasp.datastores.TopicCategory

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
) extends DatastoreModel[TopicCategory]

object MultiTopicModel {
  def fromTopicModels(name: String, topicNameField: String, topicModels: Seq[TopicModel]): MultiTopicModel = {
    areTopicsHealthy(topicModels).fold(
      s => throw new IllegalArgumentException(s),
      _ => ()
    )
    new MultiTopicModel(name, topicNameField, topicModels.map(_.name))
  }

  type TopicCompressionValidationError = Map[TopicCompression, Seq[TopicModel]]

  def validateTopicModelsHaveSameCompression(topics: Seq[TopicModel]): Either[TopicCompressionValidationError, Unit] = {
    val grouped = topics.groupBy(_.topicCompression)

    if (grouped.keySet.size != 1) {
      Left(grouped)
    } else {
      Right(())
    }
  }

  def formatTopicCompressionValidationError(error: TopicCompressionValidationError): String = {

    error
      .map {
        case (compression, topics) =>
          val t = TopicCompression.asString(compression)
          topics.map(_.name).mkString(s"[", ",", s"] use $t")
      }
      .mkString("All topic models must have the same compression setting, found settings: ", ",", "")

  }

  /**
    * Checks that:
    * - there is at least one topic model
    * - the topic models are all different models
    * - the topic models refer to different topics
    * - the topic models have the same compression
    */
  private[wasp] def areTopicsHealthy(models: Seq[TopicModel]): Either[String, Unit] = {
    for {
      _ <- Either.cond(models.nonEmpty, (), "There must be at least one topic model").right
      _ <- Either.cond(models.size == models.distinct.size, (), "Each topic model can only appear once").right
      _ <- Either.cond(models.size == models.map(_.name).distinct.size, (), "Each topic can only appear once").right
      r <- validateTopicModelsHaveSameCompression(models).left.map(formatTopicCompressionValidationError).right
    } yield r
  }

  /**
    * Checks that:
    * - topics are healthy as per [[areTopicsHealthy]]
    * - the topic models have the same data type
    * - the topic models have the same schema
    */
  private[wasp] def areTopicsEqualForReading(models: Seq[TopicModel]): Either[String, Unit] = {
    for {
      _ <- areTopicsHealthy(models).right
      _ <- Either
            .cond(
              models.map(_.topicDataType).distinct.length == 1,
              (),
              "All topic models must have the same topic data type"
            )
            .right
      r <- Either
            .cond(models.map(_.getJsonSchema).distinct.size == 1, (), "All topic models must have the same schema")
            .right
    } yield r
  }

  /**
    * Checks that:
    * - topics are healthy as per [[areTopicsHealthy]]
    * - topics are equal for reading as per [[areTopicsEqualForReading]]
    * - the key field name is always the same
    * - the header field name is always the same
    * - the value field names are always the same
    */
  private[wasp] def areTopicsEqualForWriting(models: Seq[TopicModel]): Either[String, Unit] = {
    for {
      _ <- areTopicsHealthy(models).right
      _ <- areTopicsEqualForReading(models).right
      _ <- Either
            .cond(
              models.map(_.keyFieldName).distinct.length == 1,
              (),
              "All topic models must have the same key field name"
            )
            .right
      _ <- Either
            .cond(
              models.map(_.headersFieldName).distinct.length == 1,
              (),
              "All topic models must have the same headers field name"
            )
            .right
      r <- Either
            .cond(
              models.map(_.valueFieldsNames).distinct.length == 1,
              (),
              "All topic models must have the same value field names"
            )
            .right
    } yield r
  }

  private[wasp] def topicsShareKeySchema(topics: Seq[TopicModel]): Either[String, Unit] = {
    Either.cond(
      topics.map(_.keySchema).distinct.size == 1,
      (),
      s"Cannot parse data from the following topics as a single dataframe:\n\t" +
        topics.map(_.name).mkString("\t") + "\n" +
        "It is not possible since they do not share the same key schema:\n\t" +
        topics.map(t => s"${t.name} -> ${t.keySchema}").mkString("\t")
    )
  }

  def topicNameToColumnName(s: String): String = s.replaceAllLiterally(".", "_").replaceAllLiterally("-", "_")
}
