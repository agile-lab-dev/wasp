package it.agilelab.bigdata.wasp.repository.mongo.providers

import com.github.dwickern.macros.NameOf.nameOf
import SealedTraitCodecProvider.TYPE_FIELD
import it.agilelab.bigdata.wasp.models.{ContainsRawMatchingStrategy, DataStoreConf, ExactKeyValueMatchingStrategy, ExactRawMatchingStrategy, KeyValueDataStoreConf, KeyValueMatchingStrategy, KeyValueModel, NoPartitionPruningStrategy, PartitionPruningStrategy, PrefixAndTimeBoundKeyValueMatchingStrategy, PrefixKeyValueMatchingStrategy, PrefixRawMatchingStrategy, RawDataStoreConf, RawMatchingStrategy, RawModel, TimeBasedBetweenPartitionPruningStrategy}
import it.agilelab.bigdata.wasp.repository.core.dbModels.{KeyValueDBModel, KeyValueDBModelV1, RawDBModel, RawDBModelV1}
import it.agilelab.bigdata.wasp.repository.core.mappers.{KeyValueMapperSelector, KeyValueMapperV1, RawMapperSelector, RawMapperV1}
import org.bson.codecs.configuration.CodecRegistry
import org.bson.codecs.{DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}
import org.mongodb.scala.bson.BsonDocument

object DataStoreConfCodecProviders {

  object DataStoreConfCodecProvider extends SealedTraitCodecProvider[DataStoreConf] {

    override def decodeWithType(classType: String, bsonReader: BsonReader, decoderContext: DecoderContext, registry: CodecRegistry): DataStoreConf = {
      classType match {
        case KeyValueDataStoreConf.TYPE =>
          registry.get(classOf[KeyValueDataStoreConf]).decode(bsonReader, decoderContext)
        case RawDataStoreConf.TYPE =>
          registry.get(classOf[RawDataStoreConf]).decode(bsonReader, decoderContext)
      }
    }

    override def encodeWithType(bsonWriter: BsonWriter,
                                value: DataStoreConf, encoderContext: EncoderContext, registry: CodecRegistry): BsonDocument = {
      value match {
        case kvd: KeyValueDataStoreConf =>
          createBsonDocument(registry.get(classOf[KeyValueDataStoreConf]), KeyValueDataStoreConf.TYPE, kvd, encoderContext)
        case rds: RawDataStoreConf =>
          createBsonDocument(registry.get(classOf[RawDataStoreConf]), RawDataStoreConf.TYPE, rds, encoderContext)
      }
    }

    override def clazzOf: Class[DataStoreConf] = classOf[DataStoreConf]
  }

  object RawDataStoreConfCodecProvider extends AbstractCodecProvider[RawDataStoreConf] {
    override def decodeClass(registry: CodecRegistry)
                            (implicit reader: BsonReader, decoderContext: DecoderContext): RawDataStoreConf = {
      reader.readString(TYPE_FIELD)

      val inputKeyColumn = reader.readString(nameOf[RawDataStoreConf](_.inputKeyColumn))
      val corellationIdColumn = reader.readString(nameOf[RawDataStoreConf](_.correlationIdColumn))
      val rawModel: RawDBModel = readObject(nameOf[RawDataStoreConf](_.rawModel), registry.get(classOf[RawDBModel]))

      RawDataStoreConf(
        inputKeyColumn,
        corellationIdColumn,

        // automatically select corresponding mapper
        RawMapperSelector.factory(rawModel),

        readObject(nameOf[RawDataStoreConf](_.rawMatchingStrategy), registry.get(classOf[RawMatchingStrategy])),
        readObject(nameOf[RawDataStoreConf](_.partitionPruningStrategy), registry.get(classOf[PartitionPruningStrategy]))
      )
    }

    override def encodeClass(registry: CodecRegistry, rawDataStoreConf: RawDataStoreConf)
                            (implicit writer: BsonWriter, encoderContext: EncoderContext): Unit = {
      writer.writeString(TYPE_FIELD, RawDataStoreConf.TYPE)

      writer.writeString(nameOf[RawDataStoreConf](_.inputKeyColumn), rawDataStoreConf.inputKeyColumn)
      writer.writeString(nameOf[RawDataStoreConf](_.correlationIdColumn), rawDataStoreConf.correlationIdColumn)

      // always use the last mapper
      writeObject(nameOf[RawDataStoreConf](_.rawModel), RawMapperV1.transform[RawDBModelV1](rawDataStoreConf.rawModel), registry.get(classOf[RawDBModel]))
      writeObject(nameOf[RawDataStoreConf](_.rawMatchingStrategy), rawDataStoreConf.rawMatchingStrategy, registry.get(classOf[RawMatchingStrategy]))
      writeObject(nameOf[RawDataStoreConf](_.partitionPruningStrategy), rawDataStoreConf.partitionPruningStrategy, registry.get(classOf[PartitionPruningStrategy]))
    }

    override def clazzOf: Class[RawDataStoreConf] = classOf[RawDataStoreConf]
  }

  object KeyValueDataStoreConfCodecProvider extends AbstractCodecProvider[KeyValueDataStoreConf] {
    override def decodeClass(registry: CodecRegistry)
                            (implicit reader: BsonReader, decoderContext: DecoderContext): KeyValueDataStoreConf = {
      reader.readString(TYPE_FIELD)

      val inputKeyColumn = reader.readString(nameOf[KeyValueDataStoreConf](_.inputKeyColumn))
      val corellationIdColumn = reader.readString(nameOf[KeyValueDataStoreConf](_.correlationIdColumn))
      val keyValueModel = readObject(nameOf[KeyValueDataStoreConf](_.keyValueModel), registry.get(classOf[KeyValueDBModel]))

      KeyValueDataStoreConf(
        inputKeyColumn,
        corellationIdColumn,
        KeyValueMapperSelector.factory(keyValueModel),
        readObject(nameOf[KeyValueDataStoreConf](_.keyValueMatchingStrategy), registry.get(classOf[KeyValueMatchingStrategy]))
      )
    }

    override def encodeClass(registry: CodecRegistry, keyValueDataStoreConf: KeyValueDataStoreConf)
                            (implicit writer: BsonWriter, encoderContext: EncoderContext): Unit = {
      writer.writeString(TYPE_FIELD, KeyValueDataStoreConf.TYPE)

      writer.writeString(nameOf[KeyValueDataStoreConf](_.inputKeyColumn), keyValueDataStoreConf.inputKeyColumn)
      writer.writeString(nameOf[KeyValueDataStoreConf](_.correlationIdColumn), keyValueDataStoreConf.correlationIdColumn)

      writeObject(nameOf[KeyValueDataStoreConf](_.keyValueModel), KeyValueMapperV1.transform[KeyValueDBModelV1](keyValueDataStoreConf.keyValueModel), registry.get(classOf[KeyValueDBModel]))
      writeObject(nameOf[KeyValueDataStoreConf](_.keyValueMatchingStrategy), keyValueDataStoreConf.keyValueMatchingStrategy, registry.get(classOf[KeyValueMatchingStrategy]))
    }

    override def clazzOf: Class[KeyValueDataStoreConf] = classOf[KeyValueDataStoreConf]
  }

  object KeyValueMatchingStrategyCodecProvider extends SealedTraitCodecProvider[KeyValueMatchingStrategy] {

    override def decodeWithType(classType: String,
                                bsonReader: BsonReader,
                                decoderContext: DecoderContext,
                                registry: CodecRegistry): KeyValueMatchingStrategy = {
      classType match {
        case ExactKeyValueMatchingStrategy.TYPE =>
          registry.get(classOf[ExactKeyValueMatchingStrategy]).decode(bsonReader, decoderContext)
        case PrefixKeyValueMatchingStrategy.TYPE =>
          registry.get(classOf[PrefixKeyValueMatchingStrategy]).decode(bsonReader, decoderContext)
        case PrefixAndTimeBoundKeyValueMatchingStrategy.TYPE =>
          registry.get(classOf[PrefixAndTimeBoundKeyValueMatchingStrategy]).decode(bsonReader, decoderContext)
      }
    }

    override def encodeWithType(bsonWriter: BsonWriter,
                                value: KeyValueMatchingStrategy,
                                encoderContext: EncoderContext,
                                registry: CodecRegistry): BsonDocument = {
      value match {
        case exact: ExactKeyValueMatchingStrategy =>
          createBsonDocument(registry.get(classOf[ExactKeyValueMatchingStrategy]), ExactKeyValueMatchingStrategy.TYPE, exact, encoderContext)
        case prefix: PrefixKeyValueMatchingStrategy =>
          createBsonDocument(registry.get(classOf[PrefixKeyValueMatchingStrategy]), PrefixKeyValueMatchingStrategy.TYPE, prefix, encoderContext)
        case prefixAndTime: PrefixAndTimeBoundKeyValueMatchingStrategy =>
          createBsonDocument(registry.get(classOf[PrefixAndTimeBoundKeyValueMatchingStrategy]), PrefixAndTimeBoundKeyValueMatchingStrategy.TYPE, prefixAndTime, encoderContext)
      }
    }

    override def clazzOf: Class[KeyValueMatchingStrategy] = classOf[KeyValueMatchingStrategy]
  }


  object RawMatchingStrategyCodecProvider extends SealedTraitCodecProvider[RawMatchingStrategy] {

    override def decodeWithType(classType: String,
                                bsonReader: BsonReader,
                                decoderContext: DecoderContext,
                                registry: CodecRegistry): RawMatchingStrategy = {
      classType match {
        case ExactRawMatchingStrategy.TYPE =>
          registry.get(classOf[ExactRawMatchingStrategy]).decode(bsonReader, decoderContext)
        case PrefixRawMatchingStrategy.TYPE =>
          registry.get(classOf[PrefixRawMatchingStrategy]).decode(bsonReader, decoderContext)
        case ContainsRawMatchingStrategy.TYPE =>
          registry.get(classOf[ContainsRawMatchingStrategy]).decode(bsonReader, decoderContext)
      }
    }

    override def encodeWithType(bsonWriter: BsonWriter,
                                value: RawMatchingStrategy,
                                encoderContext: EncoderContext,
                                registry: CodecRegistry): BsonDocument = {
      value match {
        case exact: ExactRawMatchingStrategy =>
          createBsonDocument(registry.get(classOf[ExactRawMatchingStrategy]), ExactRawMatchingStrategy.TYPE, exact, encoderContext)
        case prefix: PrefixRawMatchingStrategy =>
          createBsonDocument(registry.get(classOf[PrefixRawMatchingStrategy]), PrefixRawMatchingStrategy.TYPE, prefix, encoderContext)
        case contains: ContainsRawMatchingStrategy =>
          createBsonDocument(registry.get(classOf[ContainsRawMatchingStrategy]), ContainsRawMatchingStrategy.TYPE, contains, encoderContext)
      }
    }

    override def clazzOf: Class[RawMatchingStrategy] = classOf[RawMatchingStrategy]
  }

  object PartitionPruningStrategyCodecProvider extends SealedTraitCodecProvider[PartitionPruningStrategy] {

    override def decodeWithType(classType: String,
                                bsonReader: BsonReader,
                                decoderContext: DecoderContext,
                                registry: CodecRegistry): PartitionPruningStrategy = {
      classType match {
        case TimeBasedBetweenPartitionPruningStrategy.TYPE =>
          registry.get(classOf[TimeBasedBetweenPartitionPruningStrategy]).decode(bsonReader, decoderContext)
        case NoPartitionPruningStrategy.TYPE =>
          registry.get(classOf[NoPartitionPruningStrategy]).decode(bsonReader, decoderContext)
      }
    }

    override def encodeWithType(bsonWriter: BsonWriter,
                                value: PartitionPruningStrategy,
                                encoderContext: EncoderContext,
                                registry: CodecRegistry): BsonDocument = {
      value match {
        case exact: TimeBasedBetweenPartitionPruningStrategy =>
          createBsonDocument(registry.get(classOf[TimeBasedBetweenPartitionPruningStrategy]), TimeBasedBetweenPartitionPruningStrategy.TYPE, exact, encoderContext)
        case prefix: NoPartitionPruningStrategy =>
          createBsonDocument(registry.get(classOf[NoPartitionPruningStrategy]), NoPartitionPruningStrategy.TYPE, prefix, encoderContext)
      }
    }

    override def clazzOf: Class[PartitionPruningStrategy] = classOf[PartitionPruningStrategy]
  }

}
