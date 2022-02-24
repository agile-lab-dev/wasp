package it.agilelab.bigdata.wasp.master.web.controllers

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import it.agilelab.bigdata.wasp.core.utils.FreeCodeCompilerUtils
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct._
import it.agilelab.bigdata.wasp.models.configuration.RestEnrichmentConfigModel
import it.agilelab.bigdata.wasp.models.editor._
import it.agilelab.bigdata.wasp.models._
import it.agilelab.bigdata.wasp.repository.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.utils.JsonSupport
import org.mongodb.scala.bson.BsonDocument
import spray.json.{JsObject, JsValue}

trait PipegraphEditorService {

  // Forward converter methods (dto -> model)
  def toPipegraphModel(dto: PipegraphDTO, isUpdate: Boolean = false): Either[List[ErrorDTO], PipegraphModel] = {

    val nameCheckResult: Option[List[ErrorDTO]] = checkPipegraphName(dto.name, isUpdate).map(List(_))

    val errs: List[ErrorDTO] =
      dto.structuredStreamingComponents.flatMap(toStructuredStreamingETLModel(_).fold(x => x, _ => List.empty)) ++
        nameCheckResult.getOrElse(List.empty)

    if (errs.nonEmpty) Left(errs)
    else
      Right(
        PipegraphModel(
          dto.name,
          dto.description,
          dto.owner.map(x => if (x.isEmpty) "ui" else x).getOrElse("ui"),
          isSystem = false,
          creationTime = System.currentTimeMillis(),
          structuredStreamingComponents =
            dto.structuredStreamingComponents.map(toStructuredStreamingETLModel(_).right.get),
          enrichmentSources = RestEnrichmentConfigModel(Map.empty)
        )
      )
  }

  def toStructuredStreamingETLModel(
      dto: StructuredStreamingETLDTO
  ): Either[List[ErrorDTO], StructuredStreamingETLModel] = {
    val errs: List[ErrorDTO] =
      toReaderModel(dto.streamingInput).fold(x => x, _ => List.empty) ++
        toWriterModel(dto.streamingOutput).fold(x => x, _ => List.empty) ++
        toStrategyModel(dto.strategy).fold(x => x, _ => List.empty)

    if (errs.nonEmpty) Left(errs)
    else
      Right(
        StructuredStreamingETLModel(
          dto.name,
          dto.group,
          streamingInput = toReaderModel(dto.streamingInput).right.get,
          staticInputs = List.empty, // !
          streamingOutput = toWriterModel(dto.streamingOutput).right.get,
          mlModels = List.empty, // !
          strategy = Some(toStrategyModel(dto.strategy).right.get),
          triggerIntervalMs = dto.triggerIntervalMs,
          options = dto.options
        )
      )
  }

  def toWriterModel(dto: WriterModelDTO): Either[List[ErrorDTO], WriterModel] = dto.datastoreModel match {
    case TopicModelDTO(name) =>
      getTopicModelById(name) match {
        case Some(x: TopicModel)      => Right(WriterModel.kafkaWriter(dto.name, x, dto.options))
        case Some(x: MultiTopicModel) => Right(WriterModel.kafkaMultitopicWriter(dto.name, x, dto.options))
        case _                     => Left(List(ErrorDTO.notFound("Topic model", name)))
      }
    case IndexModelDTO(name) =>
      getIndexModelById(name) match {
        case Some(x) => Right(WriterModel.indexWriter(dto.name, x, dto.options))
        case None    => Left(List(ErrorDTO.notFound("Index model", name)))
      }
    case KeyValueModelDTO(name) =>
      getKeyValueModelById(name) match {
        case Some(x) => Right(WriterModel.keyValueWriter(dto.name, x, dto.options))
        case None    => Left(List(ErrorDTO.notFound("Key-Value model", name)))
      }
    case RawModelDTO(name, None) =>
      getRawModelById(name) match {
        case Some(x) => Right(WriterModel.rawWriter(dto.name, x, dto.options))
        case None    => Left(List(ErrorDTO.notFound("Raw model", name)))
      }
    case RawModelDTO(name, Some(config)) =>
//      val params: RawOptions = RawOptions(
//        saveMode = config.saveMode,
//        format = config.format,
//        extraOptions = config.extraOptions,
//        partitionBy = config.partitionBy
//      )
//      val model: RawModel =
//        RawModel(name = name, uri = config.uri, timed = config.timed, schema = config.schema, options = params)

      val model = config
      upsertRawModel(model)
      Right(WriterModel.rawWriter(dto.name, model, dto.options))
    case x => Left(List(ErrorDTO.illegalArgument("Datastore model", x.toString)))
  }

  def toReaderModel(dto: ReaderModelDTO): Either[List[ErrorDTO], StreamingReaderModel] = dto.datastoreModel match {
    case TopicModelDTO(name) =>
      getTopicModelById(name) match {
        case Some(x: TopicModel) =>
          Right(
            StreamingReaderModel.kafkaReader(
              name = dto.name,
              topicModel = x,
              options = dto.options,
              rateLimit = dto.rateLimit
            )
          )
        case Some(x: MultiTopicModel) =>
          Right(
            StreamingReaderModel.kafkaReaderMultitopic(
              name = dto.name,
              multiTopicModel = x,
              options = dto.options,
              rateLimit = dto.rateLimit
            )
          )
        case _ => Left(List(ErrorDTO.notFound("Topic model", name)))
      }
    case x => Left(List(ErrorDTO.illegalArgument("Datastore model", x.toString)))
  }

  def toStrategyModel(strategy: StrategyDTO): Either[List[ErrorDTO], StrategyModel] = strategy match {
    case FreeCodeDTO(code: String, name: String, config: Option[JsObject]) =>
      val errors: List[ErrorDTO] = validateStrategyCode(code)
      if (errors.nonEmpty) Left(errors)
      else {
//        val name = Random.alphanumeric.take(10).mkString("")
        val options: String = config match {
          case None    => s"""{name:"${name}"}"""
          case Some(x) => mergeConfigsStrings(x.toString(), s"""{name:"${name}"}""")
        }
        val finName = getStringFromConfigString(options, "name")
        upsertCodeModel(FreeCodeModel(finName, code))
        Right(
          StrategyModel(
            "it.agilelab.bigdata.wasp.consumers.spark.strategies.FreeCodeStrategy",
            Some(options)
          )
        )
      }
    case FlowNifiDTO(processGroup, name: String, config: Option[JsObject]) =>
      parsePGJson(processGroup) match {
        case Some((id, errorPort, values)) =>
          upsertProcessGroup(ProcessGroupModel(id, BsonDocument.apply(processGroup), errorPort))
          val options: String = config match {
            case None    => toStrategyProcessGroup(id, errorPort, values)
            case Some(x) => mergeConfigsStrings(toStrategyProcessGroup(id, errorPort, values), x.toString())
          }
          Right(
            StrategyModel(
              "it.agilelab.bigdata.wasp.spark.plugins.nifi.NifiStrategy",
              Some(mergeConfigsStrings(options, s"""{name:"${name}"}"""))
            )
          )
        case None => Left(List(ErrorDTO.unknownArgument("process group json", "provided isn't parsable")))
      }
    case StrategyClassDTO(className, config) =>
      Right(StrategyModel(className, config.map(_.toString)))
    case _ => Left(List(ErrorDTO.unknownArgument("strategy model", "unknown type")))
  }

  // Backward converter methods (model -> dto)
  def toDTO(pipegraph: PipegraphModel): PipegraphDTO =
    PipegraphDTO(
      pipegraph.name,
      pipegraph.description,
      Some(pipegraph.owner),
      pipegraph.structuredStreamingComponents.map(toDTO)
    )

  def toDTO(etl: StructuredStreamingETLModel): StructuredStreamingETLDTO = {
    StructuredStreamingETLDTO(
      name = etl.name,
      group = etl.group,
      streamingInput = toDTO(etl.streamingInput),
      streamingOutput = toDTO(etl.streamingOutput),
      strategy = toDTO(etl.strategy.get),
      triggerIntervalMs = etl.triggerIntervalMs,
      options = etl.options
    )
  }

  def toDTO(strategy: StrategyModel): StrategyDTO = {
    import spray.json._
    strategy match {
      case StrategyModel("it.agilelab.bigdata.wasp.consumers.spark.strategies.FreeCodeStrategy", Some(config)) =>
        val id                       = getStringFromConfigString(config, "name")
        val codeModel: FreeCodeModel = getCodeModel(id).getOrElse(throw new IllegalArgumentException(id))
        FreeCodeDTO(codeModel.code, getStringFromConfigString(config, "name"), Some(config.parseJson.asJsObject))
      case StrategyModel("it.agilelab.bigdata.wasp.spark.plugins.nifi.NifiStrategy", Some(config)) =>
        val id                                   = getStringFromConfigString(config, "nifi.process-group-id")
        val processGroupModel: ProcessGroupModel = getProcessGroup(id).getOrElse(throw new IllegalArgumentException(id))
        FlowNifiDTO(processGroupModel.content.toString, getStringFromConfigString(config, "name"), Some(config.parseJson.asJsObject))
      case StrategyModel(className, config) =>
        StrategyClassDTO(className, config.map(_.parseJson.asJsObject))
    }
  }

  def toDTO(readerModel: StreamingReaderModel): ReaderModelDTO = {
    val datastore: TopicModelDTO = readerModel.datastoreProduct match {
      case GenericTopicProduct | KafkaProduct => TopicModelDTO(readerModel.datastoreModelName)
      case x                                  => throw new IllegalArgumentException(x.toString)
    }
    ReaderModelDTO(
      name = readerModel.name: String,
      datastoreModel = datastore,
      options = readerModel.options,
      rateLimit = readerModel.rateLimit
    )
  }

  def toDTO(writerModel: WriterModel): WriterModelDTO = {
    val datastore: DatastoreModelDTO = writerModel.datastoreProduct match {
      case GenericTopicProduct | KafkaProduct => TopicModelDTO(writerModel.datastoreModelName)
      case GenericIndexProduct                => IndexModelDTO(writerModel.datastoreModelName)
      case GenericKeyValueProduct             => KeyValueModelDTO(writerModel.datastoreModelName)
      case RawProduct                         => RawModelDTO(writerModel.datastoreModelName, None)
      case x                                  => throw new IllegalArgumentException(x.toString)
    }
    WriterModelDTO(
      name = writerModel.name: String,
      datastoreModel = datastore,
      options = writerModel.options
    )
  }

  def mergeConfigsStrings(left: String, right: String): String = {
    val lc = ConfigFactory.parseString(left)
    val rc = ConfigFactory.parseString(right)
    lc.withFallback(rc).resolve().root().render(ConfigRenderOptions.concise())
  }

  def getStringFromConfigString(conf: String, field: String): String =
    ConfigFactory.parseString(conf).getString(field)

  def toStrategyProcessGroup(id: String, errorPort: String, variables: JsValue): String = s"""
                                                                                             | nifi.process-group-id = "${id}"
                                                                                             | nifi.error-port = "${errorPort}"
                                                                                             | nifi.variables = ${variables}""".stripMargin

  // Database access methods
  def getAllUIPipegraphs: List[PipegraphModel]
  def getUIPipegraph(name: String): Option[PipegraphModel]
  def checkPipegraphName(name: String, isUpdate: Boolean = false): Option[ErrorDTO]
  def checkIOByName(name: String, datastore: DatastoreProduct): Option[ErrorDTO]
  def validateStrategyCode(code: String): List[ErrorDTO]
  def insertPipegraphModel(model: PipegraphModel): Unit
  def updatePipegraphModel(model: PipegraphModel): Unit
  def getTopicModelById(name: String): Option[DatastoreModel]
  def getRawModelById(name: String): Option[RawModel]
  def getIndexModelById(name: String): Option[IndexModel]
  def getKeyValueModelById(name: String): Option[KeyValueModel]
  def upsertRawModel(model: RawModel): Unit
  def upsertProcessGroup(pg: ProcessGroupModel): Unit
  def upsertCodeModel(cm: FreeCodeModel): Unit

  def getProcessGroup(name: String): Option[ProcessGroupModel]
  def getCodeModel(name: String): Option[FreeCodeModel]
  // Utilities
  def parsePGJson(json: String): Option[(String, String, JsValue)]
}

/**
  * Default implementation of Pipegraph editor service
  * @param utils used to validate free code strategy
  */
class DefaultPipegraphEditorService(val utils: FreeCodeCompilerUtils) extends PipegraphEditorService with JsonSupport {

  override def getAllUIPipegraphs: List[PipegraphModel] = ConfigBL.pipegraphBL.getByOwner("ui").toList
  override def getUIPipegraph(name: String): Option[PipegraphModel] =
    ConfigBL.pipegraphBL.getByName(name).filter(_.owner == "ui")

  override def parsePGJson(json: String): Option[(String, String, JsValue)] = {
    import spray.json._
    val parsedJson = json.parseJson

    val id: Option[String]       = parsedJson.asJsObject.fields.get("id").map(_.convertTo[String])
    val content: Option[JsValue] = parsedJson.asJsObject.fields.get("content")

    val variables: Option[JsValue] =
      content.flatMap(y => y.asJsObject.fields.get("variables"))
    val errorPort: Option[String] = content.flatMap(y =>
      y.asJsObject.fields
        .get("outputPorts")
        .map(ports => ports.convertTo[List[Map[String, JsValue]]])
        .flatMap(
          _.filter(n => n.getOrElse("name", JsString.empty) == JsString("wasp-error")).head.get("identifier")
        )
        .map(_.convertTo[String])
    )

    for (a <- id; b <- errorPort; c <- variables) yield (a, b, c)
  }

  override def checkPipegraphName(name: String, isUpdate: Boolean = false): Option[ErrorDTO] =
    (ConfigBL.pipegraphBL.getByName(name).isDefined, isUpdate) match {
      case (false, true) => Some(ErrorDTO.notFound("Pipegraph", name))
      case (true, false) => Some(ErrorDTO.alreadyExists("Pipegraph", name))
      case _             => None
    }

  override def checkIOByName(name: String, datastore: DatastoreProduct): Option[ErrorDTO] = {
    datastore match {
      case GenericIndexProduct =>
        ConfigBL.indexBL.getByName(name).map(_ => ErrorDTO.alreadyExists("Streaming IO [index]", name))
      case GenericKeyValueProduct =>
        ConfigBL.keyValueBL.getByName(name).map(_ => ErrorDTO.alreadyExists("Streaming IO [key-value]", name))
      case GenericTopicProduct =>
        ConfigBL.topicBL.getByName(name).map(_ => ErrorDTO.alreadyExists("Streaming IO [topic]", name))
      case RawProduct => ConfigBL.rawBL.getByName(name).map(_ => ErrorDTO.alreadyExists("Streaming IO [raw]", name))
      case _          => Some(ErrorDTO.unknownArgument("Datastore product", datastore.getActualProductName))
    }
  }

  override def validateStrategyCode(code: String): List[ErrorDTO] =
    utils.validate(code).map(e => ErrorDTO(e.toString()))

  override def insertPipegraphModel(model: PipegraphModel): Unit = ConfigBL.pipegraphBL.insert(model)
  override def updatePipegraphModel(model: PipegraphModel): Unit = ConfigBL.pipegraphBL.update(model)

  override def upsertProcessGroup(pg: ProcessGroupModel): Unit = ConfigBL.processGroupBL.upsert(pg)
  override def upsertCodeModel(cm: FreeCodeModel): Unit        = ConfigBL.freeCodeBL.upsert(cm)

  override def getTopicModelById(name: String): Option[DatastoreModel] = ConfigBL.topicBL.getByName(name)
  override def getRawModelById(name: String): Option[RawModel]                        = ConfigBL.rawBL.getByName(name)
  override def getIndexModelById(name: String): Option[IndexModel]                    = ConfigBL.indexBL.getByName(name)
  override def getKeyValueModelById(name: String): Option[KeyValueModel]              = ConfigBL.keyValueBL.getByName(name)

  override def getProcessGroup(name: String): Option[ProcessGroupModel] = ConfigBL.processGroupBL.getById(name)
  override def getCodeModel(name: String): Option[FreeCodeModel]        = ConfigBL.freeCodeBL.getByName(name)

  override def upsertRawModel(model: RawModel): Unit = ConfigBL.rawBL.upsert(model)
}

/**
  * Empty implementation of Pipegraph editor service to speed up test development
  */
class EmptyPipegraphEditorService() extends PipegraphEditorService {
  override def getAllUIPipegraphs: List[PipegraphModel]                                      = List.empty
  override def getUIPipegraph(name: String): Option[PipegraphModel]                          = None
  override def parsePGJson(json: String): Option[(String, String, JsValue)]                  = None
  override def checkPipegraphName(name: String, isUpdate: Boolean = false): Option[ErrorDTO] = None
  override def checkIOByName(name: String, datastore: DatastoreProduct): Option[ErrorDTO]    = None
  override def validateStrategyCode(code: String): List[ErrorDTO]                            = List.empty
  override def insertPipegraphModel(model: PipegraphModel): Unit                             = {}
  override def updatePipegraphModel(model: PipegraphModel): Unit                             = {}
  override def upsertProcessGroup(pg: ProcessGroupModel): Unit                               = {}
  override def upsertCodeModel(cm: FreeCodeModel): Unit                                      = {}
  override def getTopicModelById(name: String): Option[DatastoreModel]        = None
  override def getRawModelById(name: String): Option[RawModel]                               = None
  override def getIndexModelById(name: String): Option[IndexModel]                           = None
  override def getKeyValueModelById(name: String): Option[KeyValueModel]                     = None
  override def getProcessGroup(name: String): Option[ProcessGroupModel]                      = None
  override def getCodeModel(name: String): Option[FreeCodeModel]                             = None
  override def upsertRawModel(model: RawModel): Unit                                         = {}
}
