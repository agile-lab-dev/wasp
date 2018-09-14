package it.agilelab.bigdata.wasp.core.utils

import it.agilelab.bigdata.wasp.core.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.core.utils.ReflectionUtils.{findSubclassesOfSealedTrait, getRuntimeClass}
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}


/**
	* MongoDB codec provider for `DatastoreProduct`.
	*
	* @author Nicolò Bidotti
	*/
object DatastoreProductCodecProvider extends CodecProvider {
	private val subclassesOfDatastoreProduct = findSubclassesOfSealedTrait[DatastoreProduct].map(getRuntimeClass(_).getName).toSet
	
  private def isDatastoreProduct[T](clazz: Class[T]): Boolean = subclassesOfDatastoreProduct(clazz.getName)

	override def get[T](clazz: Class[T], registry: CodecRegistry): Codec[T] = {
		if (isDatastoreProduct(clazz)) {
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
			decodingLookupMap((category, product))
		}
			
		override def encode(writer: BsonWriter, value: DatastoreProduct, encoderContext: EncoderContext): Unit = {
			val (category, product) = encodingLookupMap(value)
			writer.writeStartDocument()
			writer.writeString(categoryField, category)
			writer.writeString(productField, product)
			writer.writeEndDocument()
		}
		
		override def getEncoderClass: Class[DatastoreProduct] = DatastoreProduct.getClass.asInstanceOf[Class[DatastoreProduct]]
	}
}