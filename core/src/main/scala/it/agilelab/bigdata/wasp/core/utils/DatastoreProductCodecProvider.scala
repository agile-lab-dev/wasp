package it.agilelab.bigdata.wasp.core.utils

import it.agilelab.bigdata.wasp.core.datastores.{DatastoreCategory, DatastoreProduct}
import org.bson.{BsonInvalidOperationException, BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}



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
			val objectSubclassesList = ReflectionUtils.findObjectSubclassesOfSealedTraitAssumingTheyAreAllObjects[DatastoreProduct]
			
			// build list of (object, identifier)
			val subclassesIdentifiersList = objectSubclassesList.map(datastoreProduct => {
					val identifier = datastoreProduct.category + "|" + datastoreProduct.product.getOrElse("")
					(datastoreProduct, identifier)
	      }
			)
			
			// check that there's only one pipe
			require(!subclassesIdentifiersList.map(_._2).exists(_.count(_ == '|') > 1), "The character '|' cannot be used in product or category names!")
			
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