package com.mongodb.spark.sql

import java.util

import com.mongodb.client.MongoCollection
import com.mongodb.client.model._
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ForeachWriter, Row}
import org.bson.BsonDocument

import scala.collection.JavaConverters._

class MongoForeachRddWriter (writeConfig: WriteConfig, schema: StructType) extends ForeachWriter[Row] {

  var mongoConnector: MongoConnector = _
  var mapper: Row => BsonDocument = _
  var fieldNames: Seq[String] = _
  var queryKeyList: Seq[String] = _
  var batch: util.ArrayList[BsonDocument] = _



  override def open(partitionId: Long, version: Long): Boolean = {
    mongoConnector = MongoConnector(writeConfig.asOptions)
    mapper = MapFunctions.rowToDocumentMapper(schema, writeConfig.extendedBsonTypes)
    fieldNames = schema.fieldNames.toList
    queryKeyList = BsonDocument.parse(writeConfig.shardKey.getOrElse("{_id: 1}")).keySet().asScala.toList
    batch = new util.ArrayList[BsonDocument](writeConfig.maxBatchSize)
    true
  }

  override def process(value: Row): Unit = {

    batch.add(mapper(value))

    if(batch.size()>=writeConfig.maxBatchSize) {
      writeBatch
    }
  }

  private def writeBatch = {
    mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[BsonDocument] =>

      val updateOptions = new UpdateOptions().upsert(true)
      val requests: util.List[_ <: WriteModel[BsonDocument]] = batch.asScala.map(doc =>
        if (queryKeyList.forall(doc.containsKey(_)) ) {
          val queryDocument = new BsonDocument()
          queryKeyList.foreach(key => queryDocument.append(key, doc.get(key)))
          if (writeConfig.replaceDocument) {
            new ReplaceOneModel[BsonDocument](queryDocument, doc, updateOptions)
          } else {
            queryDocument.keySet().asScala.foreach(doc.remove(_))
            new UpdateOneModel[BsonDocument](queryDocument, new BsonDocument("$set", doc), updateOptions)
          }
        } else {
          new InsertOneModel[BsonDocument](doc)
        }).asJava

      collection.bulkWrite(requests, new BulkWriteOptions().ordered(writeConfig.ordered))
      batch.clear()
    })
  }

  override def close(errorOrNull: Throwable): Unit = {

    if(batch.size()>0){
      writeBatch
    }
    batch.clear()
    mongoConnector.close()
  }
}


object MongoForeachRddWriter{
  def apply(sparkConf: SparkConf, schema: StructType): MongoForeachRddWriter = new MongoForeachRddWriter(WriteConfig(sparkConf), schema)
}