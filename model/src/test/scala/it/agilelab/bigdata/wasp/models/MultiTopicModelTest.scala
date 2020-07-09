package it.agilelab.bigdata.wasp.models

import org.mongodb.scala.bson.BsonDocument
import org.scalatest.FunSuite

class MultiTopicModelTest extends FunSuite {

  test("Test that topic model compression validation works") {

    val topicForNameAndCompression = (name: String, compression: TopicCompression) =>
      TopicModel(name = name,
                 creationTime = 0L,
                 partitions = 1,
                 replicas = 1,
                 topicDataType = "json",
                 keyFieldName = None,
                 headersFieldName = None,
                 valueFieldsNames = None,
                 useAvroSchemaManager = false,
                 schema = BsonDocument(),
                 topicCompression = compression)



    val topicsWithSameCompression = Vector.range(0, 10).map(x => (s"topic$x", TopicCompression.Gzip)).map(topicForNameAndCompression.tupled)

    val sameCompressionResult = MultiTopicModel.validateTopicModelsHaveSameCompression(topicsWithSameCompression)

    assert(sameCompressionResult == Right(()))

    val topicsWithDifferentCompression = Vector.range(0, 4).map{
      case x if x%2 == 1 => (s"topic$x", TopicCompression.Gzip)
      case x if x%2 == 0 => (s"topic$x", TopicCompression.Snappy)
    }.map(topicForNameAndCompression.tupled)

    val differentCompressionResult = MultiTopicModel.validateTopicModelsHaveSameCompression(topicsWithDifferentCompression)

    assert(differentCompressionResult.isLeft)

    val message = MultiTopicModel.formatTopicCompressionValidationError(differentCompressionResult.left.get)


    assert(message === "All topic models must have the same compression setting, found settings: [topic0,topic2] use snappy,[topic1,topic3] use gzip")


    MultiTopicModel.validateTopicModels(topicsWithSameCompression)

    assertThrows[IllegalArgumentException](MultiTopicModel.validateTopicModels(topicsWithDifferentCompression))

  }

}
