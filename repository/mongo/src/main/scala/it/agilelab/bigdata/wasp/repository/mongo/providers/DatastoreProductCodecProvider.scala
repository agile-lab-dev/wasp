package it.agilelab.bigdata.wasp.repository.mongo.providers

import it.agilelab.bigdata.wasp.datastores.{DatastoreProduct, GenericProduct}
import it.agilelab.bigdata.wasp.utils.DatastoreProductSerde
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}

/**
	* MongoDB codec provider for `DatastoreProduct`.
	*
	* @author Nicolò Bidotti
	*/
object DatastoreProductCodecProvider extends CodecProvider {

	override def get[T](clazz: Class[T], registry: CodecRegistry): Codec[T] = {
		if (classOf[DatastoreProduct].isAssignableFrom(clazz)) {
			DatastoreProductCodec.asInstanceOf[Codec[T]]
		} else {
			null
		}
	}

	object DatastoreProductCodec extends DatastoreProductSerde with Codec[DatastoreProduct] {
		override def decode(reader: BsonReader, decoderContext: DecoderContext): DatastoreProduct = {
			reader.readStartDocument()
			val category = reader.readString(categoryField)
			val product = reader.readString(productField)
			reader.readEndDocument()
			product match {
				case "unknown" => GenericProduct(category, None)
				case _         => GenericProduct(category, Option(product))
			}
		}

		override def encode(writer: BsonWriter, value: DatastoreProduct, encoderContext: EncoderContext): Unit = {
			writer.writeStartDocument()
			writer.writeString(categoryField, value.categoryName)
			writer.writeString(productField, value.productName.getOrElse("unknown"))
			writer.writeEndDocument()
		}

		override def getEncoderClass: Class[DatastoreProduct] = DatastoreProduct.getClass.asInstanceOf[Class[DatastoreProduct]]
	}
}
