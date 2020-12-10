package it.agilelab.bigdata.wasp.consumers.spark.http.utils

import java.util.regex.{Matcher, Pattern}

object HttpEnricherUtils {

  private val parametersPattern = Pattern.compile("\\$\\{(.*?)\\}")

  def resolveUrlPath(
                      path: String,
                      params: Map[String, String]
                    ): String = {

    val pathVars =
      matcherToIterator(
        parametersPattern.matcher(path)
      ).toList

    if (pathVars.toSet.diff(params.keySet).isEmpty) {
      val formattedParams: Set[FormattedPathParams] =
        params.map {
          case (k, v) => FormattedPathParams(k, v)
        }.toSet
      resolveVarsPath(path, pathVars, formattedParams)
    } else {
      throw new Exception(s"Input and string variables do not match (path: $path)")
    }
  }

  private def matcherToIterator(matcher: Matcher): Iterator[String] =
    new Iterator[String] {
      override def hasNext: Boolean = matcher.find()
      override def next(): String = matcher.group(1)
    }

  private def resolveVarsPath(
                               path: String,
                               pathVars: List[String],
                               params: Set[FormattedPathParams]
                             ): String = {
    params.filter(
        formattedVars => pathVars.contains(formattedVars.varKey)
    ).foldLeft(path){
      case (z, fv) => z.replaceAllLiterally(fv.formattedVarKey, fv.valueKey)
    }
  }

  def mergeHeaders(
                    paramHeaders: Map[String, String],
                    configHeaders: Map[String, String]
                  ): Map[String, String] = {
    paramHeaders.keySet.union(configHeaders.keySet)
      .map{
        name => {
          paramHeaders.get(name) match {
            case Some(value) => (name, value)
            case None => (name, configHeaders(name))
          }
        }
      }.toMap
  }
}
