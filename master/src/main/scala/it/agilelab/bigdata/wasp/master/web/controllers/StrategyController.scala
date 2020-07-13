package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper.getJsonArrayOrEmpty
import it.agilelab.bigdata.wasp.utils.JsonSupport
import org.reflections.Reflections
import org.reflections.scanners.SubTypesScanner
import org.reflections.util.{ClasspathHelper, ConfigurationBuilder}
import spray.json._

object StrategyController extends Directives with JsonSupport {
  lazy val reflections = new Reflections(new ConfigurationBuilder()
      .setUrls(ClasspathHelper.forClassLoader)
    .setScanners(new SubTypesScanner(false)))


  def getRoute: Route = {
    pathPrefix("strategy") {
      parameters('pretty.as[Boolean].?(false)) { (pretty: Boolean) =>
        pathEnd {
          get {
            complete {
              val clazzFilter = Class.forName("it.agilelab.bigdata.wasp.consumers.spark.strategies.InternalStrategy")
              val output = reflections.getSubTypesOf(Class.forName("it.agilelab.bigdata.wasp.consumers.spark.strategies.Strategy"))
                .toArray().map{c=>c.asInstanceOf[Class[_]] }.filterNot(o=> clazzFilter.isAssignableFrom(o)).map(_.getName)
              getJsonArrayOrEmpty[String](output, _.toJson, pretty)
            }
          }
        }
      }
    }
  }

}
