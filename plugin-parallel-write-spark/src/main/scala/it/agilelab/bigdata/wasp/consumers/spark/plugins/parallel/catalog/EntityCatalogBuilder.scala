package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog

import it.agilelab.bigdata.wasp.core.utils.ConfigManager

import scala.util.Try


/**
  * Builds a microservice catalog service
  */
trait EntityCatalogBuilder {
  def getEntityCatalogService(): EntityCatalogService

  /**
    * Gets microservice catalog class name from configurations, instantiate the class and cast to MicroServiceCatalogService
    * @param configPath Path in configurations containing MicroserviceCatalogService class name
    * @tparam T         Type of microservice
    * @return           Instance of MicroserviceCatalogService
    */
  protected def getEntityCatalogService(configPath: String): EntityCatalogService = {
    val className: String = ConfigManager.conf.getString(configPath)
    if (className == null || className == "") throw new Exception("Empty or null catalog-class name")
    getEntityCatalogServiceInstance(className).recover{
      case classNotFoundException: ClassNotFoundException => throw new ClassNotFoundException(s"No class with name '$className\' found")
      case reflectiveOperationException: ReflectiveOperationException => throw new InstantiationException(s"Could not create instance of $className")
      case classCastException: ClassCastException => throw new ClassCastException(s"$className does not extend MicroserviceCatalogService")
      case _ => throw new Exception("Can't create MicroserviceCatalogService")
    }.get
  }

  private def getEntityCatalogServiceInstance(className: String): Try[EntityCatalogService] = for {
    clazz <-getClass(className)
    serviceInstance <- instantiateService(clazz)
  } yield serviceInstance

  private def instantiateService(clazz: Class[_]): Try[EntityCatalogService] = Try(clazz.getDeclaredConstructor().newInstance().asInstanceOf[EntityCatalogService])
  private def getClass(className: String): Try[Class[_]] = Try(Class.forName(className))
}

/**
  * MicroserviceCatalogBuilder companion object
  */
object EntityCatalogBuilder extends EntityCatalogBuilder {
  private val configPath = "plugin.microservice-catalog.catalog-class"

  /**
    * Obtain platform catalog service class from configuration and instantiate it by reflection
    * @tparam T Type of microservice getted by catalog service
    * @return   Instance of microservice catalog service
    */
  def getEntityCatalogService(): EntityCatalogService = {
    getEntityCatalogService(configPath)
  }
}
