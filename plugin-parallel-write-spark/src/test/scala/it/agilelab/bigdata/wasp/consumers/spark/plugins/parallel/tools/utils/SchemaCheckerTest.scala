package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.SchemaChecker
import org.apache.spark.sql.types.{ LongType, StringType, StructField, StructType }
import org.scalatest.FunSuite
import it.agilelab.bigdata.wasp.utils.EitherUtils._

class SchemaCheckerTest extends FunSuite {

  test("isValid should throw error if expected schema has less elements than actual") {
    val expected = StructType(StructField("el1", StringType) :: StructField("el2", StringType) :: Nil)
    val actual = StructType(
      StructField("el1", StringType) :: StructField("el2", StringType) :: StructField("el3", StringType) :: Nil
    )
    val res = SchemaChecker.isValid(expected, actual)
    assert(res.isFailure)
    assert(res.left.get.getMessage.contains("Unexpected"))
  }

  test("isValid should throw error if expected schema has more elements than actual") {
    val expected = StructType(
      StructField("el1", StringType) :: StructField("el2", StringType) :: StructField("el3", StringType) :: Nil
    )
    val actual = StructType(StructField("el1", StringType) :: StructField("el2", StringType) :: Nil)
    val res    = SchemaChecker.isValid(expected, actual)
    assert(res.isFailure)
    assert(res.left.get.getMessage.contains("Missing"))
  }

  test("isValid should throw error if one element type in schema doesn't match") {
    val expected = StructType(
      StructField("el1", StringType) :: StructField("el2", StringType) :: Nil
    )
    val actual = StructType(StructField("el1", StringType) :: StructField("el2", LongType) :: Nil)
    val res    = SchemaChecker.isValid(expected, actual)
    assert(res.isFailure)
    assert(res.left.get.getMessage.contains("don't match"))
  }

  test("isValid should throw error if one element name  in schema doesn't match") {
    val expected = StructType(
      StructField("el1", StringType) :: StructField("el2", StringType) :: Nil
    )
    val actual = StructType(StructField("el1", StringType) :: StructField("el2_renamed", StringType) :: Nil)
    val res    = SchemaChecker.isValid(expected, actual)
    assert(res.isFailure)
    assert(res.left.get.getMessage.contains("don't match"))
  }

  test("isValid should return Right when schemas are equal") {
    val expected = StructType(
      StructField("el1", StringType) :: StructField("el2", StringType) :: Nil
    )
    val actual = StructType(
      StructField("el1", StringType) :: StructField("el2", StringType) :: Nil
    )
    val res = SchemaChecker.isValid(expected, actual)
    assert(res.isSuccess)
  }

  test("isValid should correctly compare equal nested schemas (nullable is compared with >=)") {
    val expected = StructType(
      StructField("el1", StringType) :: StructField(
        "el2",
        StructType(StructField("nested_el1", StringType) :: StructField("nested_el2", LongType, true) :: Nil)
      ) :: Nil
    )
    val actual = StructType(
      StructField("el1", StringType) :: StructField(
        "el2",
        StructType(StructField("nested_el1", StringType) :: StructField("nested_el2", LongType, false) :: Nil)
      ) :: Nil
    )
    val res = SchemaChecker.isValid(expected, actual)
    assert(res.isSuccess)
  }

  test("isValid should fail when expected is not nullable but actual is") {
    val expected = StructType(
      StructField("el1", StringType) :: StructField(
        "el2",
        StructType(StructField("nested_el1", StringType) :: StructField("nested_el2", LongType, false) :: Nil)
      ) :: Nil
    )
    val actual = StructType(
      StructField("el1", StringType) :: StructField(
        "el2",
        StructType(StructField("nested_el1", StringType) :: StructField("nested_el2", LongType, true) :: Nil)
      ) :: Nil
    )
    val res = SchemaChecker.isValid(expected, actual)
    assert(res.isFailure)
    assert(res.left.get.getMessage.contains("don't match"))
  }

  test("isValid should fail on wrong nested schemas") {
    val expected = StructType(
      StructField("el1", StringType) :: StructField(
        "el2",
        StructType(StructField("nested_el1", StringType) :: StructField("nested_el2", LongType) :: Nil)
      ) :: Nil
    )
    val actual = StructType(
      StructField("el1", StringType) :: StructField(
        "el2",
        StructType(StructField("nested_el1", StringType) :: StructField("nested_el2", StringType) :: Nil)
      ) :: Nil
    )
    val res = SchemaChecker.isValid(expected, actual)
    assert(res.isFailure)
    assert(res.left.get.getMessage.contains("don't match"))
  }

  test("isSelectable should fail when column names are not a superset") {
    val expected =
      StructType(StructField("a", StringType) :: StructField("b", StringType) :: StructField("c", StringType) :: Nil)
    val actual = StructType(StructField("a", StringType) :: StructField("b", StringType) :: Nil)
    val res    = SchemaChecker.isSelectable(expected, actual)
    assert(res.isFailure)
    assert(res.left.get.getMessage.contains("Missing values: ('c')"))
  }

  test("isSelectable should succeed when column names are a superset") {
    val actual =
      StructType(StructField("a", StringType) :: StructField("b", StringType) :: StructField("c", StringType) :: Nil)
    val expected = StructType(StructField("a", StringType) :: StructField("b", StringType) :: Nil)
    val res      = SchemaChecker.isSelectable(expected, actual)
    assert(res.isSuccess)
  }

  test("isSelectable should fail when it encounters duplicate columns in expected position") {
    val duplicate =
      StructType(StructField("a", StringType) :: StructField("a", StringType) :: StructField("c", StringType) :: Nil)
    val correct = StructType(StructField("a", StringType) :: StructField("b", StringType) :: Nil)
    val res      = SchemaChecker.isSelectable(duplicate, correct)
    assert(res.isFailure)
    assert(res.left.get.getMessage.contains("Duplicate columns in target schema"))
  }


  test("isSelectable should fail when it encounters duplicate columns in actual position") {
    val duplicate =
      StructType(StructField("a", StringType) :: StructField("a", StringType) :: StructField("c", StringType) :: Nil)
    val correct = StructType(StructField("a", StringType) :: StructField("b", StringType) :: Nil)
    val res      = SchemaChecker.isSelectable(correct, duplicate)
    assert(res.isFailure)
    assert(res.left.get.getMessage.contains("Duplicate columns in source schema"))
  }

  test("isSelectable should be case insensitive") {
    val source =
      StructType(StructField("A", StringType) :: StructField("b", StringType) :: Nil)
    val target = StructType(StructField("a", StringType) :: StructField("b", StringType) :: Nil)
    val res      = SchemaChecker.isSelectable(target, source)
    assert(res.isSuccess)
  }

  test("isSelectable should fail when it encounters columns with same name with different case in source schema") {
    val source =
      StructType(StructField("A", StringType) :: StructField("a", StringType) :: StructField("b", StringType) :: Nil)
    val target = StructType(StructField("a", StringType) :: StructField("b", StringType) :: Nil)
    val res      = SchemaChecker.isSelectable(target, source)
    assert(res.isFailure)
    assert(res.left.get.getMessage.contains("More columns with same name in different case found in source schema"))
  }
  test("isSelectable should fail when it encounters columns with same name with different case in target schema") {
    val source =
      StructType(StructField("a", StringType) :: StructField("b", StringType) :: Nil)
    val target = StructType(StructField("A", StringType) :: StructField("a", StringType) :: StructField("b", StringType) :: Nil)
    val res      = SchemaChecker.isSelectable(target, source)
    assert(res.isFailure)
    assert(res.left.get.getMessage.contains("More columns with same name in different case found in target schema"))
  }
}
