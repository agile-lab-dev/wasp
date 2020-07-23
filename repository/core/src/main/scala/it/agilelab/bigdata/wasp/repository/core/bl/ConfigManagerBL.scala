package it.agilelab.bigdata.wasp.repository.core.bl

import it.agilelab.bigdata.wasp.models.Model

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag

trait ConfigManagerBL {

  def getByName[T <: Model](name : String)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[T]

  def retrieveConf[T <: Model](default: T, nameConf: String)(implicit ct: ClassTag[T], typeTag: TypeTag[T]): Option[T]

  def retrieveDBConfig(): Seq[String]

}


