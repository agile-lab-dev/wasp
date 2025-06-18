package it.agilelab.bigdata.utils

import org.scalatest.{FunSuite, Matchers}

class NonEmptyListSpec extends FunSuite with Matchers {

  test("size should return correct size for different list lengths") {
    assert(NonEmptyList(1, Nil).size == 1)
    assert(NonEmptyList(1, List(2)).size == 2)
    assert(NonEmptyList(1, List(2, 3, 4, 5)).size == 5)
  }

  test("map should correctly transform elements") {
    val nel    = NonEmptyList(1, List(2, 3))
    val mapped = nel.map(_ * 2)
    assert(mapped == NonEmptyList(2, List(4, 6)))
  }

  test("map should handle identity function") {
    val nel = NonEmptyList(1, List(2, 3))
    assert(nel.map(identity) == nel)
  }

  test("map should work with different types") {
    val nel    = NonEmptyList(1, List(2, 3))
    val mapped = nel.map(_.toString)
    assert(mapped == NonEmptyList("1", List("2", "3")))
  }

  test("mkString should concatenate elements with separator") {
    assert(NonEmptyList("a", List("b", "c")).mkString(",") == "a,b,c")
    assert(NonEmptyList(1, List(2, 3)).mkString(" - ") == "1 - 2 - 3")
  }

  test("mkString should handle empty tail correctly") {
    assert(NonEmptyList("only", Nil).mkString(",") == "only")
  }

  test("exists should return true if the head matches the predicate") {
    assert(NonEmptyList(1, List(2, 3)).exists(_ == 1))
  }

  test("exists should return true if an element in the tail matches the predicate") {
    assert(NonEmptyList(1, List(2, 3)).exists(_ == 3))
  }

  test("exists should return false if no elements match the predicate") {
    assert(!NonEmptyList(1, List(2, 3)).exists(_ == 10))
  }

  test("exists should handle always-true and always-false predicates") {
    val nel = NonEmptyList(1, List(2, 3))
    assert(nel.exists(_ => true))   // Always true predicate
    assert(!nel.exists(_ => false)) // Always false predicate
  }

  test("one should create a NonEmptyList with a single element") {
    val nel = NonEmptyList.one(42)
    assert(nel.head == 42)
    assert(nel.tail.isEmpty)
    assert(nel.size == 1)
  }

  test("one should support different types") {
    val nel = NonEmptyList.one("Scala")
    assert(nel.head == "Scala")
    assert(nel.tail.isEmpty)
  }
}
