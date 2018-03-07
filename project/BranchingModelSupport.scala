import scala.util.Failure

object BranchingModelSupport {

  import scala.util.{Success, Try}

  type ParseResult = Either[String, Reference]
  type FormatResult = Either[String, String]


  def versionForContainingRepositoryOrGitlabCi(baseVersion: BaseVersion): String = {
    val parsedFromCi: ParseResult = parseVersion(fromCi)

    parsedFromCi
      .left.map(error => parseVersion(fromGit).left.map(_ + "\n" + error)).joinLeft
      .right
      .flatMap(format(baseVersion)) match {
      case Left(message) => throw new RuntimeException(message)
      case Right(version) => version
    }


  }

  def fromCi: () => Try[String] = () => Option(System.getenv("CI_COMMIT_REF_NAME")) match {
    case Some(ref) => Success(ref)
    case None => Failure(new Exception("No CI_COMMIT_REF_NAME defined"))
  }

  def versionForContainingRepository(baseVersion: BaseVersion): String = {
    parseVersion(fromGit).right.flatMap(format(baseVersion)) match {
      case Left(message) => throw new RuntimeException(message)
      case Right(version) => version
    }
  }

  def fromGit: () => Try[String] = () => Try {

    import scala.sys.process.Process

    val process = Process("git rev-parse --abbrev-ref HEAD")

    process.lineStream.head
  }

  def versionForConstant(constant: String)(baseVersion: BaseVersion) = {
    parseVersion(fromConstant(constant)).right.flatMap(format(baseVersion)) match {
      case Left(message) => throw new RuntimeException(message)
      case Right(version) => version
    }
  }

  def parseVersion(referenceReader: () => Try[String]): ParseResult = {

    import References._

    val develop = """develop""".r
    val otherBranches = """(feature|release|hotfix)/(.+)""".r("kind", "rest")
    val releaseVersionString = """v([0-9]+)\.([0-9]+)""".r("major", "minor")
    val tag = """v([0-9]+)\.([0-9]+)\.([0-9]+)""".r("major", "minor", "patch")

    def unparseableReference(reference: String) = Left(s"Could not parse [$reference] branch to a wasp version, are " +
      s"you in a git repository?")

    def unretrievableReference(reference: Throwable) = Left(s"Could not retrieve branch, are you in a git repository? " +
      s"[${reference.getMessage}]")

    referenceReader().map[ParseResult] {
      case develop() => Right(Develop)
      case tag(major, minor, patch) => Right(Tag(major.toInt, minor.toInt, patch.toInt))
      case reference@otherBranches(kind, name) => kind match {
        case "feature" => Right(Feature(name))
        case "hotfix" => Right(Hotfix(name))
        case "release" => name match {
          case releaseVersionString(major, minor) => Right(Release(major.toInt, minor.toInt))
          case _ => unparseableReference(reference)
        }
        case _ => unparseableReference(reference)
      }
      case reference => unparseableReference(reference)
    }.recover[ParseResult] {
      case exception => unretrievableReference(exception)
    }.get

  }

  def format(baseVersion: BaseVersion)(reference: Reference): FormatResult = {

    import References._

    import scala.util.{Left, Right}


    def valid(component: Int*) = component.toSeq == baseVersion.productIterator.take(component.length).toSeq

    def invalid(component: Int*) = !valid(component: _*)

    def formatMessage(reference: String, components: Int*) = {

      val c = components.mkString("(", ",", ")")
      val v = baseVersion.productIterator.mkString("(", ",", ")")

      s"$reference should have $c matching with base version $v"
    }

    reference match {
      case Develop =>
        Right(s"${baseVersion.major}.${baseVersion.minor}.${baseVersion.patch}-SNAPSHOT")
      case Hotfix(name) =>
        Right(s"${baseVersion.major}.${baseVersion.minor}.${baseVersion.patch}-$name-SNAPSHOT")
      case Feature(name) =>
        Right(s"${baseVersion.major}.${baseVersion.minor}.${baseVersion.patch}-$name-SNAPSHOT")
      case Release(major, minor) if invalid(major, minor) =>
        Left(formatMessage("Release branch", major, minor))
      case Release(major, minor) if valid(major, minor) =>
        Right(s"${baseVersion.major}.${baseVersion.minor}.${baseVersion.patch}-SNAPSHOT")
      case Tag(major, minor, patch) if invalid(major, minor, patch) =>
        Left(formatMessage("Tag", major, minor, patch))
      case Tag(major, minor, patch) if valid(major, minor, patch) =>
        Right(s"${baseVersion.major}.${baseVersion.minor}.${baseVersion.patch}")
    }

  }

  def fromConstant(constant: String): () => Success[String] = () => Success(constant)

  sealed trait Reference {

  }

  sealed case class BaseVersion(major: Int, minor: Int, patch: Int)


  object References {


    /**
      * Case class modeling the version of a Feature branch
      *
      * @param name The name of the feature branch
      */
    sealed case class Feature private(name: String) extends Reference


    /**
      * Case class modeling the version of a Tag
      *
      * @param major The major version
      * @param minor The minor version
      * @param patch The patch version
      */
    sealed case class Tag private(major: Int, minor: Int, patch: Int) extends Reference


    /**
      * Case class modeling a release branch
      *
      * @param major The major version
      * @param minor The minor version
      */
    sealed case class Release private(major: Int, minor: Int) extends Reference


    /**
      * Case class modeling an Hotfix branch
      *
      * @param name The name of the hotfix branch
      */
    sealed case class Hotfix private(name: String) extends Reference

    /**
      * Case class modeling Develop branch
      */
    case object Develop extends Reference

  }


}