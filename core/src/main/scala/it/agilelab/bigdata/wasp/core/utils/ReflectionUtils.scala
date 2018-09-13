package it.agilelab.bigdata.wasp.core.utils

import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => runtimeUniverse}

/**
 * @author Nicolò Bidotti
 */
private[wasp] object ReflectionUtils {
	def getClassSymbol[T: TypeTag]: ClassSymbol = {
		val tpe = runtimeUniverse.typeOf[T]
		val clazz = tpe.typeSymbol.asClass
		
		clazz
	}
	
  def findSubclassesOfSealedTrait[T: TypeTag]: List[ClassSymbol] = {
		// get the class symbol
    val clazz = getClassSymbol[T]

    // ensure we're working with a sealed trait
    require(clazz.isSealed && clazz.isTrait, "Finding all subclasses only works for sealed traits!")

    // reflection on the class to find all object subtypes
    val subclasses = clazz
	    .knownDirectSubclasses
	    .toList
	    .map(_.asClass)
    
    subclasses
  }
	
	// how to be too verbose 101
	def findObjectSubclassesOfSealedTraitAssumingTheyAreAllObjects[T: TypeTag]: List[T] = {
		// get a runtime classloader mirror
		val runtimeMirror = runtimeUniverse.runtimeMirror(this.getClass.getClassLoader)
		
		// find all subclasses
		val subclasses = findSubclassesOfSealedTrait[T]
		
		// assume they're all objects and just grab the module reference
		val objects = subclasses.map(subclass => {
				val module = runtimeMirror
					.staticModule(subclass.fullName)
					.asInstanceOf[T]
				
				module
			}
		)
		
		objects
	}
}
