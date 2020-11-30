package it.agilelab.bigdata.wasp.core.utils

object Utils {
  def using[A <: AutoCloseable, B](a: A)(f: A => B): B =
    try {
      f(a)
    } finally {
      a.close()
    }

}
