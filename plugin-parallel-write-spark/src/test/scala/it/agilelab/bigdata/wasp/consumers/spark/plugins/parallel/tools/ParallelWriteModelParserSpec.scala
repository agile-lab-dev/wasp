package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.CatalogCoordinates
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model.ParallelWriteModelParser.{parseParallelWriteModel, writerDetailsFormat}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model.{ContinuousUpdate, ParallelWrite, ParallelWriteModel, WriterDetails}
import it.agilelab.bigdata.wasp.datastores.GenericProduct
import it.agilelab.bigdata.wasp.models.GenericModel
import org.mongodb.scala.bson.BsonDocument
import org.scalatest.FunSuite
import org.scalatest.Matchers.{an, be}
import spray.json._

class ParallelWriteModelParserSpec extends FunSuite {

  test("Flavour parser") {
    val value =
      """{"writerType": "parallelWrite",
        |"saveMode": "append"
        |}""".stripMargin

    val parsed = value.parseJson.convertTo[WriterDetails]
    println(parsed)
    assert(true)
  }

  test("ParallelWrite writer") {
    lazy val genericModel = GenericModel(
      name = "test-generic",
      value = BsonDocument(
        """{
          |"writerDetails": {
          | "writerType": "parallelWrite",
          | "saveMode": "append"
          |},
          |"entityDetails": {"name":"mock"}
          |}""".stripMargin),
      product = GenericProduct("parallelWrite", None)
    )
    val model: ParallelWriteModel = parseParallelWriteModel(genericModel)
    val expectedModel = ParallelWriteModel(ParallelWrite("append"), CatalogCoordinates("", "mock", ""))
    assert(model == expectedModel)
  }
  test("Continuous update model") {
    lazy val genericModel = GenericModel(
      name = "test-generic",
      value = BsonDocument(
        """{"entityDetails": {"name":"mock"},
          |"writerDetails": {
          | "writerType": "continuousUpdate",
          | "keys": ["pk"],
          | "orderingExpression": "pk",
          |}
          |}""".stripMargin),
      product = GenericProduct("parallelWrite", None)
    )
    val model = parseParallelWriteModel(genericModel)
    val expectedModel = ParallelWriteModel(ContinuousUpdate(List("pk"), "pk"), CatalogCoordinates("", "mock", ""))
    assert(model == expectedModel)
  }

  test("Wrong generic model kind JSON") {
    lazy val genericModel = GenericModel(
      name = "test-generic",
      value = BsonDocument(
        """{"mode": "append",
          |"partitionBy": [],
          |"entityDetails": {"name":"mock"},
          |"s3aEndpoint": "localhost:4566",
          |"deltaTableDetails": {
          | "keys": ["pk"],
          | "orderingExpression": "pk",
          |}
          |}""".stripMargin),
      product = GenericProduct("wrongCategory", None)
    )
    an[IllegalArgumentException] should be thrownBy parseParallelWriteModel(genericModel)
  }

  test("Wrong value JSON") {
    lazy val genericModel = GenericModel(
      name = "test-generic",
      value = BsonDocument(
        """{"mode": "append",
          |"partitionBy": [],
          |"entityDetails": {"name":"mock"},
          |"s3aEndpointdasd": "localhost:4566",
          |"deltaTableDetails": {
          | "keys": ["pk"],
          | "orderingExpression": "pk",
          |}
          |}""".stripMargin),
      product = GenericProduct("parallelWrite", None)
    )
    an[Exception] should be thrownBy parseParallelWriteModel(genericModel)
  }
}
