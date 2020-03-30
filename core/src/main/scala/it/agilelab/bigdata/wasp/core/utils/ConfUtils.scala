package it.agilelab.bigdata.wasp.core.utils

import com.typesafe.config.{Config, ConfigRenderOptions, ConfigValueType}
import scala.collection.JavaConverters._
import scala.language.implicitConversions

object ConfUtils {
  implicit def rightBiased[A, B](e: Either[A, B]): Either.RightProjection[A, B] = e.right

  def sequence[A, B](toSequence: List[Either[A, B]]): Either[A, List[B]] = {
    toSequence.foldRight(Right(List.empty[B]): Either[A, List[B]]) {
      case (Right(a), Right(z)) => Right(a :: z)
      case (Left(a), Right(_)) => Left(a)
      case (_, l@Left(_)) => l
    }
  }

  def getString(c: Config, path: String): Either[String, String] = {
    if (c.hasPath(path) && c.getValue(path).valueType() == ConfigValueType.STRING) {
      Right(c.getString(path))
    } else {
      Left(s"Cannot find [string] $path inside ${c.root().render(ConfigRenderOptions.concise())}")
    }
  }

  def getInt(c: Config, path: String): Either[String, Int] = {
    if (c.hasPath(path) && c.getValue(path).valueType() == ConfigValueType.NUMBER) {
      Right(c.getInt(path))
    } else {
      Left(s"Cannot find [int] $path inside ${c.root().render(ConfigRenderOptions.concise())}")
    }
  }

  def getLong(c: Config, path: String): Either[String, Long] = {
    if (c.hasPath(path) && c.getValue(path).valueType() == ConfigValueType.NUMBER) {
      Right(c.getLong(path))
    } else {
      Left(s"Cannot find [long] $path inside ${c.root().render(ConfigRenderOptions.concise())}")
    }
  }

  def getConfigList(c: Config, path: String): Either[String, List[Config]] = {
    if (c.hasPath(path) && c.getValue(path).valueType() == ConfigValueType.LIST) {
      Right(c.getConfigList(path).asScala.toList)
    } else {
      Left(s"Cannot find [long] $path inside ${c.root().render(ConfigRenderOptions.concise())}")
    }
  }

}
