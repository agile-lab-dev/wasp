package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog

import it.agilelab.bigdata.wasp.core.utils.ConfigManager

import scala.util.Try


/**
  * Builds a microservice catalog service
  */
trait MicroserviceCatalogBuilder {
  def getMicroserviceCatalogService(): MicroserviceCatalogService

  /**
    * Gets microservice catalog class name from configurations, instantiate the class and cast to MicroServiceCatalogService
    * @param configPath Path in configurations containing MicroserviceCatalogService class name
    * @tparam T         Type of microservice
    * @return           Instance of MicroserviceCatalogService
    */
  protected def getMicroserviceCatalogService(configPath: String): MicroserviceCatalogService = {
    val className: String = ConfigManager.conf.getString(configPath)
    if (className == null || className == "") throw new Exception("Empty or null catalog-class name")
    getMicroserviceCatalogServiceInstance(className).recover{
      case classNotFoundException: ClassNotFoundException => throw new ClassNotFoundException(s"No class with name '$className\' found")
      case reflectiveOperationException: ReflectiveOperationException => throw new InstantiationException(s"Could not create instance of $className")
      case classCastException: ClassCastException => throw new ClassCastException(s"$className does not extend MicroserviceCatalogService")
      case _ => throw new Exception("Can't create MicroserviceCatalogService")
    }.get
  }

  private def getMicroserviceCatalogServiceInstance(className: String): Try[MicroserviceCatalogService] = for {
    clazz <-getClass(className)
    serviceInstance <- instantiateService(clazz)
  } yield serviceInstance

  private def instantiateService(clazz: Class[_]): Try[MicroserviceCatalogService] = Try(clazz.getDeclaredConstructor().newInstance().asInstanceOf[MicroserviceCatalogService])
  private def getClass(className: String): Try[Class[_]] = Try(Class.forName(className))
}

/**
  * MicroserviceCatalogBuilder companion object
  */
object MicroserviceCatalogBuilder extends MicroserviceCatalogBuilder {
  private val configPath = "plugin.microservice-catalog.catalog-class"

  /**
    * Obtain platform catalog service class from configuration and instantiate it by reflection
    * @tparam T Type of microservice getted by catalog service
    * @return   Instance of microservice catalog service
    */
  def getMicroserviceCatalogService(): MicroserviceCatalogService = {
    getMicroserviceCatalogService(configPath)
  }
}
