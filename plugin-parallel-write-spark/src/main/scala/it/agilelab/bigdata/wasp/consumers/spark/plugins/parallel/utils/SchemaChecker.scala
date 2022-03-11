package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils

import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.utils.EitherUtils._
import org.apache.spark.sql.types.{ ArrayType, DataType, StructField, StructType }

import scala.util.Try

object SchemaChecker extends Logging {

  /**
   * Checks if source schema matches target schema and returns Either an exception with
   * rich information about the differences between schemas or ()
   * @param target Schema that must be enforced
   * @param source source schema of the data
   */
  def isValid(target: StructType, source: StructType): Try[Unit] = {
    val zippedSchemas        = target.map(Some(_)).zipAll(source.map(Some(_)), None, None)
    val zippedIndexedSchemas = zippedSchemas.indices.zip(zippedSchemas)
    val errors               = zippedIndexedSchemas.map { case (i, (e, a)) => validateField(i, e, a) }.collect { case Left(e) => e }
    errors.size match {
      case 0 => Right(())
      case _ => Left(new Exception(s"""Target schema and source schema don't match, errors:
                                      |${errors.mkString("\n")}
                                      |Compared schemas:
                                      |Target:
                                      |${target.treeString}
                                      |Source:
                                      |${source.treeString}
                                      |""".stripMargin))
    }
  }

  /**
   * Checks if source schema contains all column names contained in the target schema, the objective is understand
   * if sourceDf.select(target.map(_.name).map(_col) is possible.
   * @param target Schema that must be enforced
   * @param source source schema of the data
   */
  def isSelectable(target: StructType, source: StructType): Try[Unit] =
    for {
      _ <- Either.cond(
            target.toSet.size == target.size,
            (),
            new Exception(s"Duplicate columns in target schema ${target.treeString}")
          )
      _ <- Either.cond(
            source.toSet.size == source.size,
            (),
            new Exception(s"Duplicate columns in source schema ${source.treeString}")
          )
      missingColumns = target.map(_.name).toSet -- source.map(_.name).toSet
      _ <- Either.cond(
            missingColumns.isEmpty,
            (),
            new Exception(
              s"""Source schema doesn't contain all elements of the target schema. Missing values: (${missingColumns
                   .map(c => s"'$c'")
                   .mkString(",")})
          Compared schemas:
                 |Target:
                 |${target.treeString}
                 |Source:
                 |${source.treeString}
                 |""".stripMargin
            )
          )
    } yield ()

  /**
   * Nullability is not valid when target is not nullable but source is.
   * We should never write nulls in non nullable fields, while writing to nullable fields is always available
   * @param target
   * @param source
   * @return
   */
  private def validateNullability(target: Boolean, source: Boolean): Boolean = {
    !source || target
  }

  private def validateField(
    index: Int,
    maybeTarget: Option[StructField],
    maybeSource: Option[StructField]
  ): Either[String, Unit] =
    (maybeTarget, maybeSource) match {
      case (Some(target), Some(source))
          if target.name == source.name && validateNullability(target.nullable, source.nullable) && dataTypeValid(
            target.dataType,
            source.dataType
          ) =>
        Right(())
      case (Some(target), Some(source)) =>
        Left(s"Elements at index $index don't match target $target, source $source")
      case (Some(target), None) =>
        Left(s"Missing element at index $index, target $target")
      case (None, Some(source)) =>
        Left(s"Unexpected element at index $index, target schema doesn't contain anymore elements but found $source")
      case (None, None) => {
        logger.warn(s"Schema checker reached unexpected condition checking type None with None at index $index")
        Right(())
      }
    }

  /**
   * Checks if 2 DataTypes are equal without considering nullable flag
   */
  private def dataTypeValid(target: DataType, source: DataType): Boolean =
    (target, source) match {
      case (StructType(fields1), StructType(fields2)) =>
        fields1.length == fields2.length && fields1.zip(fields2).forall {
          case (targetInnerField, sourceInnerField) =>
            targetInnerField.name == sourceInnerField.name &&
              validateNullability(targetInnerField.nullable, sourceInnerField.nullable) &&
              dataTypeValid(targetInnerField.dataType, sourceInnerField.dataType)
        }
      case (ArrayType(t1, _), ArrayType(t2, _)) => dataTypeValid(t1, t2)
      case _                                    => target == source
    }
}
