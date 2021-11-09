package it.agilelab.bigdata.wasp.repository.mongo

import com.mongodb.ErrorCategory
import com.mongodb.client.model.{CreateCollectionOptions, IndexOptions}
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.utils._
import it.agilelab.bigdata.wasp.models._
import it.agilelab.bigdata.wasp.models.configuration._
import it.agilelab.bigdata.wasp.repository.core.db.WaspDB
import it.agilelab.bigdata.wasp.repository.core.dbModels._
import it.agilelab.bigdata.wasp.repository.mongo.providers.DataStoreConfCodecProviders._
import it.agilelab.bigdata.wasp.repository.mongo.providers.VersionedRegistry._
import it.agilelab.bigdata.wasp.repository.mongo.providers._
import it.agilelab.bigdata.wasp.repository.mongo.utils.MongoDBHelper
import it.agilelab.bigdata.wasp.repository.mongo.utils.MongoDBHelper._
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}
import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros.createCodecProviderIgnoreNone
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.bson.{BsonDocument, BsonObjectId, BsonString, BsonValue}
import org.mongodb.scala.gridfs.GridFSBucket
import org.mongodb.scala.model.Indexes
import org.mongodb.scala.result.UpdateResult
import org.mongodb.scala.{MongoCommandException, MongoDatabase, MongoWriteException, Observable}

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

trait WaspMongoDB extends MongoDBHelper with WaspDB {

  def upsert[T <: Model](doc: T)(implicit ct: ClassTag[T], typeTag: TypeTag[T])

  def getAll[T <: Model]()(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Seq[T]

  def getDocumentByField[T <: Model](field: String, value: BsonValue)(
      implicit ct: ClassTag[T],
      typeTag: TypeTag[T]
  ): Option[T]

  def getDocumentByQueryParams[T <: Model](query: Map[String, BsonValue], sort: Option[BsonDocument])(
      implicit ct: ClassTag[T],
      typeTag: TypeTag[T]
  ): Option[T]

  def getAllDocumentsByField[T <: Model](field: String, value: BsonValue)(
      implicit ct: ClassTag[T],
      typeTag: TypeTag[T]
  ): Seq[T]

  def getAllRaw[T <: Model]()(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Seq[BsonDocument]

  def getDocumentByFieldRaw[T <: Model](field: String, value: BsonValue)(
      implicit ct: ClassTag[T],
      typeTag: TypeTag[T]
  ): Option[BsonDocument]

  def getDocumentByQueryParamsRaw[T <: Model](
      query: Map[String, BsonValue]
  )(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[BsonDocument]

  def getAllDocumentsByFieldRaw[T <: Model](field: String, value: BsonValue)(
      implicit ct: ClassTag[T],
      typeTag: TypeTag[T]
  ): Seq[BsonDocument]

  def insert[T <: Model](doc: T)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Unit

  def insertRaw[T <: Model](doc: BsonDocument)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Unit

  def insertIfNotExists[T <: Model](doc: T)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Unit

  def deleteByName[T <: Model](name: String)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Unit

  def deleteByQuery[T <: Model](query: Map[String, BsonValue])(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Unit

  def updateByName[T <: Model](name: String, doc: T)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): UpdateResult

  def updateByNameRaw[T <: Model](name: String, doc: BsonDocument)(
      implicit ct: ClassTag[T],
      typeTag: TypeTag[T]
  ): UpdateResult

  def saveFile(arrayBytes: Array[Byte], file: String, metadata: BsonDocument): BsonObjectId

  def deleteFileById(id: BsonObjectId): Unit

  def close(): Unit

  def getFileByID(id: BsonObjectId): Array[Byte]

}

class WaspDBMongoImp(val mongoDatabase: MongoDatabase) extends WaspMongoDB {

  import WaspMongoDB._

  /**
    * initializes collections.
    *
    * Collections are initialized concurrently by different nodes so each node tries to create it and backs off
    * if another node concurrently created the collections.
    *
    * To force name as key of models an index with unique constraint is concurrently created, if another node concurrently
    * created the index the current node backs off.
    */
  def initializeCollections() {

    val collections = collectionsLookupTable.values.toSet

    val collectionOptions = new CreateCollectionOptions()

    val COLLECTION_ALREADY_EXISTS = 48

    val results = collections
      .map(collection => (collection, Try(mongoDatabase.createCollection(collection, collectionOptions).results())))
      .map {
        //everything is fine
        case (collectionName: String, Success(_)) => Right(collectionName)
        //collection already exist, nothing to do
        case (collectionName: String, Failure(ex: MongoCommandException))
            if ex.getErrorCode == COLLECTION_ALREADY_EXISTS =>
          Right(collectionName)
        //collection correctly created
        case (_, Failure(ex: MongoCommandException)) if ex.getErrorCode != COLLECTION_ALREADY_EXISTS => Left(ex)
      }

    val failures = results.filter(_.isLeft)

    if (failures.nonEmpty) {
      val message = failures
        .map(_.left.get)
        .map(_.toString)
        .mkString(System.lineSeparator())

      throw new Exception(message)
    }

    val createdCollections = results.filter(_.isRight).map(_.right.get)

    val indexOptions = new IndexOptions().unique(true)

    val INDEX_ALREADY_EXISTS = 68

    val indexResults = createdCollections
      .map(collectionName =>
        (
          collectionName,
          Try(
            mongoDatabase
              .getCollection(collectionName)
              .createIndex(indexKeys(collectionName), indexOptions)
              .results()
          )
        )
      )
      .map {
        //everything is fine
        case (collectionName: String, Success(_)) => Right(collectionName)
        //collection already exist, nothing to do
        case (collectionName: String, Failure(ex: MongoCommandException)) if ex.getErrorCode == INDEX_ALREADY_EXISTS =>
          Right(collectionName)
        //collection correctly created
        case (_, Failure(ex: MongoCommandException)) if ex.getErrorCode != INDEX_ALREADY_EXISTS => Left(ex)
      }

    val indexFailures = indexResults.filter(_.isLeft)

    if (indexFailures.nonEmpty) {
      val message = indexFailures
        .map(_.left.get)
        .map(_.toString)
        .mkString(System.lineSeparator())

      throw new Exception(message)
    }

  }

  def getAll[T <: Model]()(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Seq[T] = {
    getAllDocuments[T](collectionsLookupTable(typeTag.tpe))
  }

  def getDocumentByField[T <: Model](
      field: String,
      value: BsonValue
  )(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[T] = {
    getDocumentByKey[T](field, value, collectionsLookupTable(typeTag.tpe))
  }

  def getDocumentByQueryParams[T <: Model](
      query: Map[String, BsonValue],
      sort: Option[BsonDocument]
  )(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[T] = {
    getDocumentByQueryParams[T](query, sort, collectionsLookupTable(typeTag.tpe))
  }

  def getAllDocumentsByField[T <: Model](
      field: String,
      value: BsonValue
  )(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Seq[T] = {
    getAllDocumentsByKey[T](field, value, collectionsLookupTable(typeTag.tpe))
  }

  def getAllRaw[T <: Model]()(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Seq[BsonDocument] = {
    getAllDocuments[BsonDocument](collectionsLookupTable(typeTag.tpe))
  }

  def getDocumentByFieldRaw[T <: Model](
      field: String,
      value: BsonValue
  )(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[BsonDocument] = {
    getDocumentByKey[BsonDocument](field, value, collectionsLookupTable(typeTag.tpe))
  }

  def getDocumentByQueryParamsRaw[T <: Model](
      query: Map[String, BsonValue]
  )(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[BsonDocument] = {
    getDocumentByQueryParams[BsonDocument](query, None, collectionsLookupTable(typeTag.tpe))
  }

  def getAllDocumentsByFieldRaw[T <: Model](
      field: String,
      value: BsonValue
  )(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Seq[BsonDocument] = {
    getAllDocumentsByKey[BsonDocument](field, value, collectionsLookupTable(typeTag.tpe))
  }

  def insert[T <: Model](doc: T)(implicit ct: ClassTag[T], typeTag: TypeTag[T]) = {
    addDocumentToCollection(collectionsLookupTable(typeTag.tpe), doc)
  }

  def upsert[T <: Model](doc: T)(implicit ct: ClassTag[T], typeTag: TypeTag[T]) = {

    logger.info(s"Upserting model '${doc.name}'")

    replaceDocumentToCollection("name", BsonString(doc.name), doc, collectionsLookupTable(typeTag.tpe), upsert = true)

    Unit
  }

  def insertIfNotExists[T <: Model](doc: T)(implicit ct: ClassTag[T], typeTag: TypeTag[T]) = {

    try {
      mongoDatabase.getCollection[T](collectionsLookupTable(typeTag.tpe)).insertOne(doc).results()
    } catch {
      case ex: MongoWriteException if ex.getError.getCategory == ErrorCategory.DUPLICATE_KEY =>
        logger.info("document already present, doing nothing")
    }

    Unit
  }

  def saveFile(arrayBytes: Array[Byte], file: String, metadata: BsonDocument): BsonObjectId = {
    BsonObjectId(
      GridFSBucket(mongoDatabase)
        .uploadFromObservable(file, Observable(List(ByteBuffer.wrap(arrayBytes))))
        .headResult()
    )
  }

  def deleteFileById(id: BsonObjectId): Unit = GridFSBucket(mongoDatabase).delete(id).headResult()

  def enumerateFile(file: String): Array[Byte] = {
    val downloadObservable = GridFSBucket(mongoDatabase).downloadToObservable(file)
    val gridFile           = downloadObservable.getGridFSFile.headResult()

    val length = gridFile.getLength
    // MUST be less than 4GB
    require(length < Integer.MAX_VALUE, s"File $file is bigger than 4GB")
    downloadObservable.headResult().array()
  }

  def close() = {
    MongoDBHelper.close()
  }

  def getFileByID(id: BsonObjectId): Array[Byte] = {

    logger.info(s"Locating file by id $id")
    val downloadObservable = GridFSBucket(mongoDatabase).downloadToObservable(id)
    val gridFile           = downloadObservable.getGridFSFile.headResult()
    val length             = gridFile.getLength
    // MUST be less than 4GB
    require(length < Integer.MAX_VALUE, s"File with id $id is bigger than 4GB")
    downloadObservable.headResult().array()
  }

  override def deleteByName[T <: Model](name: String)(implicit ct: ClassTag[T], typeTag: universe.TypeTag[T]): Unit =
    removeDocumentFromCollection[T]("name", BsonString(name), collectionsLookupTable(typeTag.tpe))

  override def deleteByQuery[T <: Model](
      query: Map[String, BsonValue]
  )(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Unit = {
    removeDocumentFromCollectionByQuery[T](BsonDocument(query), collectionsLookupTable(typeTag.tpe))
  }

  override def updateByName[T <: Model](
      name: String,
      doc: T
  )(implicit ct: ClassTag[T], typeTag: universe.TypeTag[T]): UpdateResult =
    replaceDocumentToCollection[T]("name", BsonString(name), doc, collectionsLookupTable(typeTag.tpe))

  override def insertRaw[T <: Model](doc: BsonDocument)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Unit = {

    val collection = collectionsLookupTable(typeTag.tpe)
    logger.info(s"Adding document to collection $collection")

    try {
      getCollection(collection).insertOne(doc).headResult()
      logger.info(s"Document correctly added $doc")
    } catch {
      case e: Exception =>
        logger.error(s"Unable to add document. Error message: ${e.getMessage}")
        throw e
    }
  }

  override def updateByNameRaw[T <: Model](
      name: String,
      doc: BsonDocument
  )(implicit ct: ClassTag[T], typeTag: universe.TypeTag[T]): UpdateResult =
    replaceDocumentToCollection("name", BsonString(name), doc, collectionsLookupTable(typeTag.tpe))
}

object WaspMongoDB extends Logging {
  private var waspDB: WaspMongoDB = _

  val pipegraphsName        = "pipegraphs"
  val producersName         = "producers"
  val topicsName            = "topics"
  val indexesName           = "indexes"
  val rawName               = "raw"
  val cdcName               = "cdc"
  val keyValueName          = "keyvalues"
  val sqlSourceName         = "sqlsource"
  val batchjobName          = "batchjobs"
  val batchjobInstanceName  = "batchjobinstances"
  val pipegraphInstanceName = "pipegraphinstances"

  val configurationsName  = "configurations"
  val mlModelsName        = "mlmodels"
  val websocketsName      = "websockets"
  val batchSchedulersName = "batchschedulers"
  val documentName        = "document"
  val freeCodeName        = "freeCode"
  val processGroupsName   = "processGroups"
  val httpName            = "http"
  val genericName         = "generic"

  val collectionsLookupTable: Map[Type, String] = Map(
    typeTag[PipegraphDBModel].tpe            -> pipegraphsName,
    typeTag[ProducerDBModel].tpe             -> producersName,
    typeTag[TopicModel].tpe                  -> topicsName,
    typeTag[TopicDBModel].tpe                -> topicsName,
    typeTag[MultiTopicDBModel].tpe           -> topicsName,
    typeTag[IndexDBModel].tpe                -> indexesName,
    typeTag[RawDBModel].tpe                  -> rawName,
    typeTag[CdcDBModel].tpe                  -> cdcName,
    typeTag[KeyValueDBModel].tpe             -> keyValueName,
    typeTag[SqlSourceDBModel].tpe            -> sqlSourceName,
    typeTag[BatchJobDBModel].tpe             -> batchjobName,
    typeTag[MlDBModelOnlyInfo].tpe           -> mlModelsName,
    typeTag[WebsocketDBModel].tpe            -> websocketsName,
    typeTag[BatchSchedulerDBModel].tpe       -> batchSchedulersName,
    typeTag[BatchJobInstanceDBModel].tpe     -> batchjobInstanceName,
    typeTag[PipegraphInstanceDBModel].tpe    -> pipegraphInstanceName,
    typeTag[KafkaConfigDBModel].tpe          -> configurationsName,
    typeTag[SparkBatchConfigDBModel].tpe     -> configurationsName,
    typeTag[SparkStreamingConfigDBModel].tpe -> configurationsName,
    typeTag[ElasticConfigDBModel].tpe        -> configurationsName,
    typeTag[SolrConfigDBModel].tpe           -> configurationsName,
    typeTag[HBaseConfigDBModel].tpe          -> configurationsName,
    typeTag[JdbcConfigDBModel].tpe           -> configurationsName,
    typeTag[TelemetryConfigDBModel].tpe      -> configurationsName,
    typeTag[NifiConfigDBModel].tpe           -> configurationsName,
    typeTag[DocumentDBModel].tpe             -> documentName,
    typeTag[FreeCodeDBModel].tpe             -> freeCodeName,
    typeTag[ProcessGroupDBModel].tpe         -> processGroupsName,
    typeTag[CompilerConfigDBModel].tpe       -> configurationsName,
    typeTag[HttpDBModel].tpe                 -> httpName,
    typeTag[GenericDBModel].tpe              -> genericName
  )

  lazy val indexKeys: Map[String, Bson] = collectionsLookupTable.map {
    case (typ, name) => (name, Indexes.ascending("name"))
  }.toMap ++ Map(mlModelsName -> Indexes.ascending("name", "version", "timestamp"))

  private lazy val codecProviders: java.util.List[CodecProvider] = List(
    createCodecProviderIgnoreNone[RawModel](),
    DatastoreProductCodecProvider,
    TopicCompressionCodecProvider,
    HttpCompressionCodecProvider,
    SubjectStrategyCodecProvider,
    TypesafeConfigCodecProvider,
    PipegraphInstanceDBModelProvider,
    BatchJobInstanceDBProvider,
    BatchJobModelCodecProvider,
    DocumentProvider,
    PipegraphProvider,
    ProducerDBProvider,
    TopicProvider,
    MultiTopicProvider,
    BatchJobProvider,
    IndexProvider,
    RawDBProvider,
    KeyValueProvider,
    SqlSourceProvider,
    HttpProvider,
    CdcDBProvider,
    FreeCodeProvider,
    GenericProvider,
    WebsocketProvider,
    BatchSchedulerProvider,
    ProcessGroupProvider,
    MlModelOnlyInfoProvider,
    SolrConfigProvider,
    HBaseConfigProvider,
    KafkaConfigProvider,
    ElasticConfigProvider,
    JdbcConfigProvider,
    TelemetryConfigProvider,
    SparkStreamingConfigProvider,
    ElasticConfigProvider,
    SparkBatchConfigProvider,
    NifiConfigProvider,
    CompilerConfigProvider
  ).asJava

  private lazy val gdprCodecProviders: util.List[CodecProvider] = List(
    DatastoreProductCodecProvider,
    RawDBProvider,
    createCodecProviderIgnoreNone[RawModel](),
    createCodecProviderIgnoreNone(classOf[ExactKeyValueMatchingStrategy]),
    createCodecProviderIgnoreNone(classOf[PrefixKeyValueMatchingStrategy]),
    createCodecProviderIgnoreNone(classOf[PrefixAndTimeBoundKeyValueMatchingStrategy]),
    createCodecProviderIgnoreNone(classOf[TimeBasedBetweenPartitionPruningStrategy]),
    createCodecProviderIgnoreNone(classOf[NoPartitionPruningStrategy]),
    createCodecProviderIgnoreNone(classOf[ExactRawMatchingStrategy]),
    createCodecProviderIgnoreNone(classOf[PrefixRawMatchingStrategy]),
    createCodecProviderIgnoreNone(classOf[ContainsRawMatchingStrategy]),
    PartitionPruningStrategyCodecProvider,
    RawMatchingStrategyCodecProvider,
    KeyValueMatchingStrategyCodecProvider,
    DataStoreConfCodecProvider,
    RawDataStoreConfCodecProvider,
    KeyValueDataStoreConfCodecProvider,
    BatchGdprETLModelCodecProvider,
    BatchETLCodecProvider,
    BatchJobModelCodecProvider
  ).asJava

  def initializeConnectionAndDriver(mongoDBConfig: MongoDBConfigModel): MongoDatabase = {
    val mongoDatabase = MongoDBHelper.getDatabase(mongoDBConfig)

    mongoDatabase
  }

  def getDB(): WaspMongoDB = {
    if (waspDB == null) {
      val msg = "The waspDB was not initialized"
      logger.error(msg)
      throw new Exception(msg)
    }
    waspDB
  }

  def dropDatabase(): Unit = {
    // drop db
    val mongoDBConfig = ConfigManager.getMongoDBConfig

    println(s"Dropping MongoDB database '${mongoDBConfig.databaseName}'")
    val mongoDBDatabase = MongoDBHelper.getDatabase(mongoDBConfig)
    val dropFuture      = mongoDBDatabase.drop().toFuture()
    Await.result(dropFuture, Duration(10, TimeUnit.SECONDS))
    println(s"Dropped MongoDB database '${mongoDBConfig.databaseName}'")
    System.exit(0)

    // re-initialize mongoDB and continue (instead of exit) -> not safe due to all process could write on mongoDB
    //waspDB = WaspDB.initializeDB()
  }

  def initializeDB(): WaspMongoDB = {
    // MongoDB initialization
    val mongoDBConfig = ConfigManager.getMongoDBConfig
    logger.info(
      s"Create connection to MongoDB: address ${mongoDBConfig.address}, databaseName: ${mongoDBConfig.databaseName}"
    )

    val codecRegistry = fromRegistries(
      fromProviders(codecProviders),
      fromProviders(gdprCodecProviders),
      DEFAULT_CODEC_REGISTRY
    )

    val mongoDBDatabase = initializeConnectionAndDriver(mongoDBConfig).withCodecRegistry(codecRegistry)
    val completewaspDB  = new WaspDBMongoImp(mongoDBDatabase)
    completewaspDB.initializeCollections()
    waspDB = completewaspDB
    waspDB
  }
}

object TypesafeConfigCodecProvider extends CodecProvider {

  override def get[T](clazz: Class[T], registry: CodecRegistry): Codec[T] = {
    if (classOf[Config].isAssignableFrom(clazz)) {
      ConfigCodec.asInstanceOf[Codec[T]]
    } else {
      null
    }
  }

  object ConfigCodec extends Codec[Config] {
    override def decode(reader: BsonReader, decoderContext: DecoderContext): Config = {
      val data = reader.readString()

      ConfigFactory.parseString(data)
    }

    override def encode(writer: BsonWriter, value: Config, encoderContext: EncoderContext): Unit = {
      val data = value.root().render(ConfigRenderOptions.defaults().setJson(true))
      writer.writeString(data)
    }

    override def getEncoderClass: Class[Config] = classOf[Config]
  }

}
