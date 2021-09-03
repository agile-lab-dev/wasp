/* Spark distributed-mode (Spark Standalone or Hadoop YARN cluster managers) usage !!! Add this in standalone applications !!! */
import java.io.File

(Universal / mappings) += {
  val jarsListFileName = "jars.list"

  val log = streams.value.log

  log.info("Getting jars names to use with additional-jars-lib-path config parameter (used by Wasp Core Framework)")

  // get full classpaths of all jars
  val jars = (Runtime / fullClasspath).value.map(dep => {
    val moduleOpt = dep.metadata.get(AttributeKey[ModuleID]("moduleID"))
    moduleOpt match {
      case Some(module) =>
        if (module.organization.equalsIgnoreCase("it.agilelab") || module.organization.equalsIgnoreCase("wasp-delta-lake")){
          //for some reason, the snapshot version is not appended correctly. Must do it manually
          s"${module.organization}.${module.name}-${module.revision}.jar"
        } else
          s"${module.organization}.${dep.data.getName}"

      case None =>
        log.warn(s"Dependency $dep does not have a valid ModuleID associated.")
        dep.data.getName
    }
  }).mkString("\n")

  val file = new File(IO.createTemporaryDirectory.getAbsolutePath + File.separator + jarsListFileName)
  IO.write(file, jars)

  file -> ("lib" + File.separator + jarsListFileName)
}
