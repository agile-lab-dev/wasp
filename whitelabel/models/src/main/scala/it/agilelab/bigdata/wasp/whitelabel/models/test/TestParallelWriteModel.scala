package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.CatalogCoordinates
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model.{
  ContinuousUpdate,
  ParallelWrite,
  ParallelWriteModel
}
import it.agilelab.bigdata.wasp.datastores.GenericProduct
import it.agilelab.bigdata.wasp.models.GenericModel
import org.mongodb.scala.bson.BsonDocument

object ParallelWriteModelSerde extends it.agilelab.bigdata.wasp.utils.JsonSupport {

  import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model.ParallelWriteModelParser._

  def serialize(parallelWrite: ParallelWriteModel): BsonDocument = {
    import spray.json._
    BsonDocument(parallelWrite.toJson.compactPrint)
  }

}
object TestParallelWriteModel {

  lazy val parallelWriteModel = GenericModel(
    name = "test-parquet-parallelwrite",
    value = ParallelWriteModelSerde.serialize(
      ParallelWriteModel(
        ParallelWrite("append"),
        CatalogCoordinates(
          "domain",
          "name",
          "v1"
        )
      )
    ),
    product = GenericProduct("parallelWrite", None)
  )

  lazy val continuousUpdateModel = GenericModel(
    name = "test-delta-parallelwrite",
    value = ParallelWriteModelSerde
      .serialize(
        ParallelWriteModel(
          ContinuousUpdate(
            List("id"),
            "number"
          ),
          CatalogCoordinates(
            "domain",
            "name",
            "v1"
          )
        )
      ),
    product = GenericProduct("parallelWrite", None)
  )

}
