package it.agilelab.bigdata.wasp.core.utils

import it.agilelab.bigdata.wasp.core.datastores.{DatastoreCategory, DatastoreProduct}
import org.bson.{BsonInvalidOperationException, BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}

import scala.reflect.runtime.{universe => runtimeUniverse}


/**
	* MongoDB codec provider for `DatastoreProduct`.
	*
	* @author Nicolò Bidotti
	*/
object DatastoreProductCodecProvider extends CodecProvider {
	
  private def isDatastoreProduct[T](clazz: Class[T]): Boolean = {
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
		// field for the identifier
		private val identifier = "_t"
		
		// grabs all subtypes of DatastoreProduct and builds a list of (DatastoreProcut, "category|product") from which
		// we can then build the lookup maps
		private def buildSubclassIdentifiersList(): List[(DatastoreProduct, String)] = {
			// get a runtime classloader mirror
			val runtimeMirror = runtimeUniverse.runtimeMirror(this.getClass.getClassLoader)
			
			// grab the class for the base trait
			val datastoreProductType = runtimeUniverse.typeOf[DatastoreProduct]
			val datastoreProductClass = datastoreProductType.typeSymbol.asClass
			
			// ensure we're working with a sealed trait
			require(!(datastoreProductClass.isSealed && datastoreProductClass.isTrait), "Building the lookup tables with reflection only works for sealed traits!")
			
			// reflection on the class to find all object subtypes and build identifiers for each one
			val subclassesIdentifiersList: List[(DatastoreProduct, String)] = datastoreProductClass
				.knownDirectSubclasses // finds all subclasses
		    .toList
				.map(_.getClass)
				.map(datastoreProductSubClass => {
						val productObject = runtimeMirror
							.staticModule(datastoreProductSubClass.getName)
							.asInstanceOf[DatastoreProduct with DatastoreCategory]
						val identifier = productObject.category + "|" + productObject.product.getOrElse("")
						(productObject.asInstanceOf[DatastoreProduct], identifier)
		      }
				)
			
			// check that we don't have collisions so we can use the list for forward and reverse lookup maps
			val listSize = subclassesIdentifiersList.size
			require(listSize == subclassesIdentifiersList.map(_._1).toSet.size, "There cannot be collisions between subclasses!")
			require(listSize == subclassesIdentifiersList.map(_._2).toSet.size, "There cannot be collisions between identifiers!")
			
			subclassesIdentifiersList
		}
		
		// build forward and reverse lookup maps from subclass identifiers list
		private val encodingLookupMap = buildSubclassIdentifiersList().toMap
		private val decodingLookupMap = encodingLookupMap.map(_.swap)
		
		override def decode(reader: BsonReader, decoderContext: DecoderContext): DatastoreProduct = {
			reader.readStartDocument()
			val categoryAndProduct = reader.readString(identifier)
			reader.readEndDocument()
			decodingLookupMap(categoryAndProduct)
		}
			
		override def encode(writer: BsonWriter, value: DatastoreProduct, encoderContext: EncoderContext): Unit = {
			val categoryAndProduct = encodingLookupMap(value)
			writer.writeStartDocument()
			writer.writeString(identifier, categoryAndProduct)
			writer.writeEndDocument()
		}
		
		override def getEncoderClass: Class[DatastoreProduct] = DatastoreProduct.getClass.asInstanceOf[Class[DatastoreProduct]]
	}
}