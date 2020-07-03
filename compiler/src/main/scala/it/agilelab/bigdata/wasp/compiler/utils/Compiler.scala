package it.agilelab.bigdata.wasp.compiler.utils

import java.io.File
import java.net.URLClassLoader
import java.util.jar.JarFile

import scala.reflect.internal.util.{BatchSourceFile, OffsetPosition}
import scala.reflect.io.VirtualDirectory
import scala.tools.nsc.Settings
import scala.tools.nsc.interactive.{Global, Response}

class Compiler {

  private lazy val compilerPath = try {
    jarPathOfClass("scala.tools.nsc.Interpreter")
  } catch {
    case e : Throwable =>
      throw new RuntimeException("Unable lo load scala interpreter from classpath (scala-compiler jar is missing?)", e)
  }

  private lazy val libPath = try {
    jarPathOfClass("scala.Tuple1")
  } catch {
    case e : Throwable =>
      throw new RuntimeException("Unable to load scala base object from classpath (scala-library jar is missing?)", e)
  }

  /*
 * Try to guess our app's classpath.
 * This is probably fragile.
 */
  private lazy val impliedClassPath: List[String] = {
    val currentClassPath: List[String] = this.getClass.getClassLoader.asInstanceOf[URLClassLoader].getURLs.
      map(_.toString).filter(_.startsWith("file:")).filterNot(_.contains("jaxb")).map(_.substring(5)).toList

    // if there's just one thing in the classpath, and it's a jar, assume an executable jar.
    currentClassPath ::: (if (currentClassPath.size == 1 && currentClassPath.head.endsWith(".jar")) {
      val jarFile = currentClassPath.head
      val relativeRoot = new File(jarFile).getParentFile
      val nestedClassPath = new JarFile(jarFile).getManifest.getMainAttributes.getValue("Class-Path")
      if (nestedClassPath eq null) {
        Nil
      } else {
        nestedClassPath.split(" ").map { f => new File(relativeRoot, f).getAbsolutePath }.toList
      }
    } else {
      Nil
    })
  }

  private val target = new VirtualDirectory("", None)

  private val settings = new Settings()

  private val pathList: List[String] = compilerPath ::: libPath

  settings.bootclasspath.value = pathList.mkString(File.pathSeparator)
  settings.classpath.value = (pathList ::: impliedClassPath).mkString(File.pathSeparator)

  settings.outputDirs.setSingleOutput(target)


  private val reporter = new Reporter(settings)

  protected val compiler = new Global(settings, reporter)

  private def jarPathOfClass(className: String) = try {
    val resource = className.split('.').mkString("/", "/", ".class")
    val path = getClass.getResource(resource).getPath
    val indexOfFile = path.indexOf("file:") + 5
    val indexOfSeparator = path.lastIndexOf('!')
    List(path.substring(indexOfFile, indexOfSeparator))
  }


  private def compile(code: String): BatchSourceFile = {
    val source = new BatchSourceFile("<virtual>", code)

    val response = new Response[Unit]

    compiler.askReload(List(source), response)
    response.get
    source

  }

  def reset(): Unit = {
    //nothing to do in reset now
  }


  def scopeCompletion(code: String, startPosition : Int, endPosition: Int): (List[CompletionModel], List[ErrorModel]) = {
    reporter.clear()
    reporter.setStartPosition(startPosition)
    val source: BatchSourceFile = compile(code)
    val tcompletion = new Response[List[compiler.Member]]
    val pos = compiler.ask(() => new OffsetPosition(source, endPosition))
    compiler.askScopeCompletion(pos, tcompletion)


    val vals = tcompletion.get match {
      case Left(members) =>
        compiler.ask{ () =>
          members.flatMap {
            case m: compiler.ScopeMember if m.viaImport.isEmpty && m.sym.isVal => {
                Some(CompletionModel(m.sym.nameString, m.tpe.toString()))
              }
            case _ => None
          }
        }
      case Right(e) => throw e
    }
    (vals,reporter.showMessages())
  }


  def typeCompletion(code: String, position: Int): List[CompletionModel] = {

    val source: BatchSourceFile = compile(code)

    val tcompletion = new Response[List[compiler.Member]]
    val pos = compiler.ask(() => new OffsetPosition(source, position))
    compiler.askTypeCompletion(pos, tcompletion)

    tcompletion.get match {
      case Left(members) =>
        compiler.ask { () =>
          members.map {
            case m: compiler.TypeMember if m.accessible => Some(CompletionModel(m.sym.nameString,m.tpe.toString()))
            case _ => None
          }
        }.flatten
      case Right(e) => throw e
    }
  }

  def close(): Unit = compiler.askShutdown

}