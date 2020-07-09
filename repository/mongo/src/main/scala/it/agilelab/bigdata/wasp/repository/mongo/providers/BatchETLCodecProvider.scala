package it.agilelab.bigdata.wasp.repository.mongo.providers

import com.github.dwickern.macros.NameOf._
import org.bson.codecs.configuration.CodecRegistry
import org.bson.codecs.{DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}
import org.mongodb.scala.bson.BsonDocument
import SealedTraitCodecProvider.TYPE_FIELD
import it.agilelab.bigdata.wasp.models.{BatchETL, BatchETLModel, BatchGdprETLModel, BatchJobExclusionConfig, BatchJobModel, DataStoreConf, ReaderModel, WriterModel}

object BatchETLCodecProvider extends SealedTraitCodecProvider[BatchETL] {

  override def decodeWithType(classType: String,
                              bsonReader: BsonReader,
                              decoderContext: DecoderContext,
                              registry: CodecRegistry): BatchETL = {
    classType match {
      case BatchETLModel.TYPE =>
        registry.get(classOf[BatchETLModel]).decode(bsonReader, decoderContext)
      case BatchGdprETLModel.TYPE =>
        registry.get(classOf[BatchGdprETLModel]).decode(bsonReader, decoderContext)
    }
  }

  override def encodeWithType(bsonWriter: BsonWriter,
                              value: BatchETL,
                              encoderContext: EncoderContext,
                              registry: CodecRegistry): BsonDocument = {
    value match {
      case etl: BatchETLModel =>
        createBsonDocument(registry.get(classOf[BatchETLModel]), BatchETLModel.TYPE, etl, encoderContext)
      case gdprEtl: BatchGdprETLModel =>
        createBsonDocument(registry.get(classOf[BatchGdprETLModel]), BatchGdprETLModel.TYPE, gdprEtl, encoderContext)
    }
  }

  override def clazzOf: Class[BatchETL] = classOf[BatchETL]
}


object BatchGdprETLModelCodecProvider extends AbstractCodecProvider[BatchGdprETLModel] {
  override def decodeClass(registry: CodecRegistry)
                          (implicit reader: BsonReader,
                           decoderContext: DecoderContext): BatchGdprETLModel = {
    reader.readString(TYPE_FIELD)

    BatchGdprETLModel(
      reader.readString(nameOf[BatchGdprETLModel](_.name)),
      readList[DataStoreConf](nameOf[BatchGdprETLModel](_.dataStores), registry.get(classOf[DataStoreConf])),
      reader.readString(nameOf[BatchGdprETLModel](_.strategyConfig)),
      readList[ReaderModel](nameOf[BatchGdprETLModel](_.inputs), registry.get(classOf[ReaderModel])),
      readObject[WriterModel](nameOf[BatchGdprETLModel](_.output), registry.get(classOf[WriterModel])),
      reader.readString(nameOf[BatchGdprETLModel](_.group)),
      reader.readBoolean(nameOf[BatchGdprETLModel](_.isActive))
    )
  }

  override def clazzOf: Class[BatchGdprETLModel] = classOf[BatchGdprETLModel]

  override def encodeClass(registry: CodecRegistry, batchGdprETLModel: BatchGdprETLModel)
                          (implicit writer: BsonWriter, encoderContext: EncoderContext): Unit = {

    writer.writeString(TYPE_FIELD, BatchGdprETLModel.TYPE)

    writer.writeString(nameOf[BatchGdprETLModel](_.name), batchGdprETLModel.name)
    writeList(nameOf[BatchGdprETLModel](_.dataStores), batchGdprETLModel.dataStores, registry.get(classOf[DataStoreConf]))
    writer.writeString(nameOf[BatchGdprETLModel](_.strategyConfig), batchGdprETLModel.strategyConfig)
    writeList(nameOf[BatchGdprETLModel](_.inputs), batchGdprETLModel.inputs, registry.get(classOf[ReaderModel]))
    writeObject(nameOf[BatchGdprETLModel](_.output), batchGdprETLModel.output, registry.get(classOf[WriterModel]))
    writer.writeString(nameOf[BatchGdprETLModel](_.group), batchGdprETLModel.group)
    writer.writeBoolean(nameOf[BatchGdprETLModel](_.isActive), batchGdprETLModel.isActive)

  }
}

object BatchJobModelCodecProvider extends AbstractCodecProvider[BatchJobModel] {
  override def decodeClass(registry: CodecRegistry)
                          (implicit reader: BsonReader,
                           decoderContext: DecoderContext): BatchJobModel = {
    reader.readObjectId("_id")
    BatchJobModel(
      reader.readString(nameOf[BatchJobModel](_.name)),
      reader.readString(nameOf[BatchJobModel](_.description)),
      reader.readString(nameOf[BatchJobModel](_.owner)),
      reader.readBoolean(nameOf[BatchJobModel](_.system)),
      reader.readInt64(nameOf[BatchJobModel](_.creationTime)),
      readObject(nameOf[BatchJobModel](_.etl), registry.get(classOf[BatchETL])),
      readObject(nameOf[BatchJobModel](_.exclusivityConfig), registry.get(classOf[BatchJobExclusionConfig]))
    )
  }

  override def encodeClass(registry: CodecRegistry, batchJobModel: BatchJobModel)
                          (implicit writer: BsonWriter, encoderContext: EncoderContext): Unit = {
    writer.writeString(nameOf[BatchJobModel](_.name), batchJobModel.name)
    writer.writeString(nameOf[BatchJobModel](_.description), batchJobModel.description)
    writer.writeString(nameOf[BatchJobModel](_.owner), batchJobModel.owner)
    writer.writeBoolean(nameOf[BatchJobModel](_.system), batchJobModel.system)
    writer.writeInt64(nameOf[BatchJobModel](_.creationTime), batchJobModel.creationTime)
    writeObject(nameOf[BatchJobModel](_.etl), batchJobModel.etl, registry.get(classOf[BatchETL]))
    writeObject(nameOf[BatchJobModel](_.exclusivityConfig), batchJobModel.exclusivityConfig, registry.get(classOf[BatchJobExclusionConfig]))
  }

  override def clazzOf: Class[BatchJobModel] = classOf[BatchJobModel]
}
