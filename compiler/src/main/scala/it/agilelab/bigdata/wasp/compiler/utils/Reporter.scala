package it.agilelab.bigdata.wasp.compiler.utils

import it.agilelab.bigdata.wasp.models.ErrorModel

import scala.collection.mutable
import scala.reflect.internal.{Reporter => InternalReporter}
import scala.reflect.internal.util.{FakePos, NoPosition, Position}
import scala.tools.nsc.Settings
import scala.tools.nsc.reporters.{Reporter => NSCReporter}

class Reporter(val settings: Settings) extends NSCReporter {

  private val messages : mutable.ListBuffer[ErrorModel] = mutable.ListBuffer.empty[ErrorModel]
  private val fileName = "<virtual>"
  private var startPosition : Int = 0


  def setStartPosition(startPosition  :Int) : Unit = this.startPosition = startPosition

  override protected def info0(pos: Position, msg: String, severity: Severity, force: Boolean): Unit = {
    val errorType = label(severity)
    if(errorType.isDefined) messages += showError(pos,msg,errorType.get)
  }

  private def label(severity: Severity): Option[String] = severity match {
    case InternalReporter.INFO    => None
    case InternalReporter.WARNING => Some("warning")
    case InternalReporter.ERROR   => Some("error")
  }

  def clear(): Unit = messages.clear()

  def showMessages(): List[ErrorModel] = messages.toList


  private def showError(pos : Position , msg: String, errorType: String): ErrorModel = {
    def escaped(s: String) = {
      def u(c: Int) = f"\\u$c%04x"
      def uable(c: Int) = (c < 0x20 && c != '\t') || c == 0x7F
      if (s exists (c => uable(c))) {
        val sb = new StringBuilder
        s foreach (c => sb append (if (uable(c)) u(c) else c))
        sb.toString
      } else s
    }
    def errorAt(p: Position): ErrorModel= {
      def where     = (p.line-startPosition).toString
      def content   = escaped(p.lineContent)
      def indicator = p.lineCaret
      ErrorModel(fileName,where,errorType,msg,content,indicator)
    }
    pos match {
      case FakePos(fmsg) => ErrorModel(fileName,"",errorType,s"$fmsg $msg","","")
      case NoPosition    => ErrorModel(fileName,"",errorType,msg,"","")
      case pos           => errorAt(pos)
    }
  }

}
