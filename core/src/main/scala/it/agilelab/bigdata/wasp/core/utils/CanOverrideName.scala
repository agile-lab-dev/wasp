package it.agilelab.bigdata.wasp.core.utils

trait CanOverrideName[T] {
  def named(instance: T, name: String): T
}

object CanOverrideName {
  def apply[T](f: (T, String) => T) = new CanOverrideName[T] {
    override def named(instance: T, name: String): T = f(instance, name)
  }
}
