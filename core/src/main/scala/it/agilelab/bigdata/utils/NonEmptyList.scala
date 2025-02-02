package it.agilelab.bigdata.utils

case class NonEmptyList[+A](head: A, tail: List[A]) {
  val size: Int = tail.size + 1

  def map[B](f: A => B): NonEmptyList[B] = {
    NonEmptyList(f(head), tail.map(f))
  }

  def mkString(sep: String): String = {
    if (tail.isEmpty) {
      "" + head
    } else {
      head + sep + tail.mkString(sep)
    }
  }

  def exists(p: A => Boolean): Boolean = {
    p(head) || tail.exists(p)
  }
}
object NonEmptyList {
  def one[A](a: A): NonEmptyList[A] = NonEmptyList(a, Nil)
}
