package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.ConfigManagerBL
import it.agilelab.bigdata.wasp.models.Model
import it.agilelab.bigdata.wasp.models.configuration._
import it.agilelab.bigdata.wasp.repository.core.dbModels.{CompilerConfigDBModel, CompilerConfigDBModelV1, ElasticConfigDBModel, ElasticConfigDBModelV1, HBaseConfigDBModel, HBaseConfigDBModelV1, JdbcConfigDBModel, JdbcConfigDBModelV1, KafkaConfigDBModel, KafkaConfigDBModelV1, NifiConfigDBModel, NifiConfigDBModelV1, SolrConfigDBModel, SolrConfigDBModelV1, SparkBatchConfigDBModel, SparkBatchConfigDBModelV1, SparkStreamingConfigDBModel, SparkStreamingConfigDBModelV1, TelemetryConfigDBModel, TelemetryConfigDBModelV1}
import it.agilelab.bigdata.wasp.repository.core.mappers.{CompilerConfigMapperSelector, CompilerConfigMapperV1, ElasticConfigMapperSelector, ElasticConfigMapperV1, HBaseConfigMapperSelector, HBaseConfigMapperV1, JdbcConfigMapperSelector, JdbcConfigMapperV1, KafkaConfigMapperSelector, KafkaConfigMapperV1, Mapper, NifiConfigMapperSelector, NifiConfigMapperV1, SolrConfigMapperSelector, SolrConfigMapperV1, SparkBatchConfigMapperSelector, SparkBatchConfigMapperV1, SparkStreamingConfigMapperSelector, SparkStreamingConfigMapperV1, TelemetryConfigMapperSelector, TelemetryConfigMapperV1}
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.bson.BsonString

import scala.reflect.runtime.universe._
import scala.reflect.ClassTag
import it.agilelab.bigdata.wasp.repository.mongo.utils.MongoDBHelper._

class ConfigManagerBLImpl(waspDB: WaspMongoDB) extends ConfigManagerBL{

  def getByName[T <: Model](name : String)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[T] = {

       ct.runtimeClass match {
          case solr if solr  == classOf[SolrConfigDBModel]  =>
            val config = waspDB.getDocumentByField[SolrConfigDBModel]("name", new BsonString(name))
            val mapper = SolrConfigMapperSelector.select(config.getOrElse(throw new Exception("NO VERSION FOR DB MODEL")))
            config.map(mapper.fromDBModelToModel).asInstanceOf[Option[T]]

          case hbase if hbase == classOf[HBaseConfigDBModel] =>
            val config = waspDB.getDocumentByField[HBaseConfigDBModel]("name", new BsonString(name))
            val mapper = HBaseConfigMapperSelector.select(config.getOrElse(throw new Exception("NO VERSION FOR DB MODEL")))
            config.map(mapper.fromDBModelToModel).asInstanceOf[Option[T]]

          case kafka if kafka == classOf[KafkaConfigDBModel] =>
            val config = waspDB.getDocumentByField[KafkaConfigDBModel]("name", new BsonString(name))
            val mapper = KafkaConfigMapperSelector.select(config.getOrElse(throw new Exception("NO VERSION FOR DB MODEL")))
            config.map(mapper.fromDBModelToModel).asInstanceOf[Option[T]]

          case sparkB if sparkB == classOf[SparkBatchConfigDBModel] =>
            val config = waspDB.getDocumentByField[SparkBatchConfigDBModel]("name", new BsonString(name))
            val mapper = SparkBatchConfigMapperSelector.select(config.getOrElse(throw new Exception("NO VERSION FOR DB MODEL")))
            config.map(mapper.fromDBModelToModel).asInstanceOf[Option[T]]

          case sparkS if sparkS == classOf[SparkStreamingConfigDBModel] =>
            val config = waspDB.getDocumentByField[SparkStreamingConfigDBModel]("name", new BsonString(name))
            val mapper = SparkStreamingConfigMapperSelector.select(config.getOrElse(throw new Exception("NO VERSION FOR DB MODEL")))
            config.map(mapper.fromDBModelToModel).asInstanceOf[Option[T]]

          case elastic if elastic == classOf[ElasticConfigDBModel] =>
            val config = waspDB.getDocumentByField[ElasticConfigDBModel]("name", new BsonString(name))
            val mapper = ElasticConfigMapperSelector.select(config.getOrElse(throw new Exception("NO VERSION FOR DB MODEL")))
            config.map(mapper.fromDBModelToModel).asInstanceOf[Option[T]]

          case jdbc if jdbc == classOf[JdbcConfigDBModel] =>
            val config = waspDB.getDocumentByField[JdbcConfigDBModel]("name", new BsonString(name))
            val mapper = JdbcConfigMapperSelector.select(config.getOrElse(throw new Exception("NO VERSION FOR DB MODEL")))
            config.map(mapper.fromDBModelToModel).asInstanceOf[Option[T]]

          case telemetry if telemetry == classOf[TelemetryConfigDBModel] =>
            val config = waspDB.getDocumentByField[TelemetryConfigDBModel]("name", new BsonString(name))
            val mapper = TelemetryConfigMapperSelector.select(config.getOrElse(throw new Exception("NO VERSION FOR DB MODEL")))
            config.map(mapper.fromDBModelToModel).asInstanceOf[Option[T]]

          case compiler if compiler == classOf[CompilerConfigDBModel] =>
            val config = waspDB.getDocumentByField[CompilerConfigDBModel]("name", new BsonString(name))
            val mapper = CompilerConfigMapperSelector.select(config.getOrElse(throw new Exception("NO VERSION FOR DB MODEL")))
            config.map(mapper.fromDBModelToModel).asInstanceOf[Option[T]]

          case nifi if nifi == classOf[NifiConfigDBModel] =>
            val config = waspDB.getDocumentByField[NifiConfigDBModel]("name", new BsonString(name))
            val mapper = NifiConfigMapperSelector.select(config.getOrElse(throw new Exception("NO VERSION FOR DB MODEL")))
            config.map(mapper.fromDBModelToModel).asInstanceOf[Option[T]]

          case _ => throw new Exception("ADD A CONFIG CASE HERE")
       }

  }

  def retrieveConf[T <: Model](default: T, nameConf: String)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[T] = {

    ct.runtimeClass match {
      case solr if solr == classOf[SolrConfigModel] =>
        waspDB.insertIfNotExists[SolrConfigDBModel](SolrConfigMapperV1.transform[SolrConfigDBModelV1](default.asInstanceOf[SolrConfigModel]))
        getByName[SolrConfigDBModel](nameConf).asInstanceOf[Option[T]]

      case hbase if hbase == classOf[HBaseConfigModel] =>
        waspDB.insertIfNotExists[HBaseConfigDBModel](HBaseConfigMapperV1.transform[HBaseConfigDBModelV1](default.asInstanceOf[HBaseConfigModel]))
        getByName[HBaseConfigDBModel](nameConf).asInstanceOf[Option[T]]

      case kafka if kafka == classOf[KafkaConfigModel] =>
        waspDB.insertIfNotExists[KafkaConfigDBModel](KafkaConfigMapperV1.transform[KafkaConfigDBModelV1](default.asInstanceOf[KafkaConfigModel]))
        getByName[KafkaConfigDBModel](nameConf).asInstanceOf[Option[T]]

      case sparkB if sparkB == classOf[SparkBatchConfigModel] =>
        waspDB.insertIfNotExists[SparkBatchConfigDBModel](SparkBatchConfigMapperV1.transform[SparkBatchConfigDBModelV1](default.asInstanceOf[SparkBatchConfigModel]))
        getByName[SparkBatchConfigDBModel](nameConf).asInstanceOf[Option[T]]

      case sparkS if sparkS == classOf[SparkStreamingConfigModel] =>
        waspDB.insertIfNotExists[SparkStreamingConfigDBModel](SparkStreamingConfigMapperV1.transform[SparkStreamingConfigDBModelV1](default.asInstanceOf[SparkStreamingConfigModel]))
        getByName[SparkStreamingConfigDBModel](nameConf).asInstanceOf[Option[T]]

      case elastic if elastic == classOf[ElasticConfigModel] =>
        waspDB.insertIfNotExists[ElasticConfigDBModel](ElasticConfigMapperV1.transform[ElasticConfigDBModelV1](default.asInstanceOf[ElasticConfigModel]))
        getByName[ElasticConfigDBModel](nameConf).asInstanceOf[Option[T]]

      case jdbc if jdbc == classOf[JdbcConfigModel] =>
        waspDB.insertIfNotExists[JdbcConfigDBModel](JdbcConfigMapperV1.transform[JdbcConfigDBModelV1](default.asInstanceOf[JdbcConfigModel]))
        getByName[JdbcConfigDBModel](nameConf).asInstanceOf[Option[T]]

      case telemetry if telemetry == classOf[TelemetryConfigModel] =>
        waspDB.insertIfNotExists[TelemetryConfigDBModel](TelemetryConfigMapperV1.transform[TelemetryConfigDBModelV1](default.asInstanceOf[TelemetryConfigModel]))
        getByName[TelemetryConfigDBModel](nameConf).asInstanceOf[Option[T]]

      case compiler if compiler == classOf[CompilerConfigModel] =>
        waspDB.insertIfNotExists[CompilerConfigDBModel](CompilerConfigMapperV1.transform[CompilerConfigDBModelV1](default.asInstanceOf[CompilerConfigModel]))
        getByName[CompilerConfigDBModel](nameConf).asInstanceOf[Option[T]]

      case nifi if nifi == classOf[NifiConfigModel] =>
        waspDB.insertIfNotExists[NifiConfigDBModel](NifiConfigMapperV1.transform[NifiConfigDBModelV1](default.asInstanceOf[NifiConfigModel]))
        getByName[NifiConfigDBModel](nameConf).asInstanceOf[Option[T]]
    }
  }

  def retrieveDBConfig(): Seq[String] = {
    waspDB.mongoDatabase.getCollection(WaspMongoDB.configurationsName)
      .find().results().map(_.toJson())
  }

}
