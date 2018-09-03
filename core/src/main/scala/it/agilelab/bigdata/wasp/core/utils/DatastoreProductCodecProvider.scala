package it.agilelab.bigdata.wasp.core.utils

import it.agilelab.bigdata.wasp.core.datastores.{DatastoreCategory, DatastoreProduct}
import org.bson.{BsonInvalidOperationException, BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}

import scala.reflect.runtime.{universe => ru}


/**
	* MongoDB codec provider for `DatastoreProduct`.
	*
	* @author Nicolò Bidotti
	*/
object DatastoreProductCodecProvider extends CodecProvider {
	import DatastoreProduct._
	
  def isDatastoreProduct[T](clazz: Class[T]): Boolean = {
    DatastoreProduct.getClass.isAssignableFrom(clazz)
  }

	override def get[T](clazz: Class[T], registry: CodecRegistry): Codec[T] = {
		if (isDatastoreProduct(clazz)) {
			DatastoreProductCodec.asInstanceOf[Codec[T]]
		} else {
			null
		}
	}
	
	object DatastoreProductCodec extends Codec[DatastoreProduct] {
		val identifier = "_t"
		
		val datastoreProductType = ru.typeOf[DatastoreProduct]
		val datastoreProductClass = datastoreProductType.typeSymbol.asClass
		require(!(datastoreProductClass.isSealed && datastoreProductClass.isTrait), "Building the lookup tables with reflection only works for sealed traits!")
		val encodingLookupMap = datastoreProductClass.knownDirectSubclasses
			.map(_.getClass)
  	  .map { datastoreProductSubClass =>
		    
		    val productObject = ru.runtimeMirror(this.getClass.getClassLoader).staticModule(datastoreProductSubClass.getName).asInstanceOf[DatastoreProduct with DatastoreCategory]
				
		    (productObject.asInstanceOf[DatastoreProduct], productObject.category + "|" + productObject.product.getOrElse(""))
      }
	    .toMap
		val decodingLookupMap = encodingLookupMap.map(_.swap)
		
		override def decode(reader: BsonReader, decoderContext: DecoderContext): DatastoreProduct = {
			reader.readStartDocument()
			val enumName = reader.readString(identifier)
			reader.readEndDocument()
			decodingLookupMap(enumName)
		}
			
		override def encode(writer: BsonWriter, value: DatastoreProduct, encoderContext: EncoderContext): Unit = {
			val product = encodingLookupMap.get(value).get
			
			writer.writeStartDocument()
			writer.writeString(identifier, product)
			writer.writeEndDocument()
		}
		
		override def getEncoderClass: Class[DatastoreProduct] = DatastoreProduct.getClass.asInstanceOf[Class[DatastoreProduct]]
		
	}
}