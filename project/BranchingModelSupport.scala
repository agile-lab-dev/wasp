import scala.util.Failure

object BranchingModelSupport {

  import scala.util.{Success, Try}

  type ParseResult  = Either[String, Reference]
  type FormatResult = Either[String, String]

  def versionForContainingRepositoryOrGitlabCi(baseVersion: BaseVersion, flavor: Flavor): String = {
    val parsedFromCi: ParseResult = parseVersion(fromCi)

    parsedFromCi.left
      .map(error => parseVersion(fromGit).left.map(_ + "\n" + error))
      .joinLeft
      .right
      .flatMap(format(baseVersion, flavor)) match {
      case Left(message)  => throw new RuntimeException(message)
      case Right(version) => version
    }

  }

  def fromCi: () => Try[String] =
    () =>
      Option(System.getenv("CI_COMMIT_REF_NAME")) match {
        case Some(ref) => Success(ref)
        case None      => Failure(new Exception("No CI_COMMIT_REF_NAME defined"))
      }

  def versionForContainingRepository(baseVersion: BaseVersion, flavor: Flavor): String = {
    parseVersion(fromGit).right.flatMap(format(baseVersion, flavor)) match {
      case Left(message)  => throw new RuntimeException(message)
      case Right(version) => version
    }
  }

  def fromGit: () => Try[String] =
    () =>
      Try {

        import scala.sys.process.Process

        val process = Process("git rev-parse --abbrev-ref HEAD")

        process.lineStream.head match {
          case "HEAD" => Process("git rev-parse HEAD").lineStream.head
          case s      => s
        }
      }

  def versionForConstant(constant: String, flavor: Flavor)(baseVersion: BaseVersion) = {
    parseVersion(fromConstant(constant)).right.flatMap(format(baseVersion, flavor)) match {
      case Left(message)  => throw new RuntimeException(message)
      case Right(version) => version
    }
  }

  def parseVersion(referenceReader: () => Try[String]): ParseResult = {

    import References._

    val develop              = """develop""".r
    val otherBranches        = """(feature|release|hotfix)/(.+)""".r("kind", "rest")
    val releaseVersionString = """v([0-9]+)\.([0-9]+)""".r("major", "minor")
    val tag                  = """v([0-9]+)\.([0-9]+)\.([0-9]+)""".r("major", "minor", "patch")

    def unparseableReference(reference: String) =
      Left(
        s"Could not parse [$reference] branch to a wasp version, are " +
          s"you in a git repository?"
      )

    def unretrievableReference(reference: Throwable) =
      Left(
        s"Could not retrieve branch, are you in a git repository? " +
          s"[${reference.getMessage}]"
      )

    referenceReader()
      .map[ParseResult] {
        case develop()                => Right(Develop)
        case tag(major, minor, patch) => Right(Tag(major.toInt, minor.toInt, patch.toInt))
        case reference @ otherBranches(kind, name) =>
          kind match {
            case "feature" => Right(Feature(name))
            case "hotfix"  => Right(Hotfix(name))
            case "release" =>
              name match {
                case releaseVersionString(major, minor) => Right(Release(major.toInt, minor.toInt))
                case _                                  => unparseableReference(reference)
              }
            case _ => unparseableReference(reference)
          }
        case reference => Right(Detached(reference))
      }
      .recover[ParseResult] {
        case exception => unretrievableReference(exception)
      }
      .get

  }

  def format(baseVersion: BaseVersion, flavor: Flavor)(reference: Reference): FormatResult = {

    import References._

    import scala.util.{Left, Right}

    val postfix = flavor.postfix.map("-" + _).getOrElse("")

    def valid(component: Int*) = component.toSeq == baseVersion.productIterator.take(component.length).toSeq

    def invalid(component: Int*) = !valid(component: _*)

    def formatMessage(reference: String, components: Int*) = {

      val c = components.mkString("(", ",", ")")
      val v = baseVersion.productIterator.mkString("(", ",", ")")

      s"$reference should have $c matching with base version $v"
    }

    reference match {
      case Develop =>
        Right(s"${baseVersion.major}.${baseVersion.minor}.${baseVersion.patch}${postfix}-SNAPSHOT")
      case Hotfix(name) =>
        Right(s"${baseVersion.major}.${baseVersion.minor}.${baseVersion.patch}${postfix}-$name-SNAPSHOT")
      case Feature(name) =>
        Right(s"${baseVersion.major}.${baseVersion.minor}.${baseVersion.patch}-${name}${postfix}-SNAPSHOT")
      case Release(major, minor) if invalid(major, minor) =>
        Left(formatMessage("Release branch", major, minor))
      case Release(major, minor) if valid(major, minor) =>
        Right(s"${baseVersion.major}.${baseVersion.minor}.${baseVersion.patch}-${postfix}SNAPSHOT")
      case Tag(major, minor, patch) if invalid(major, minor, patch) =>
        Left(formatMessage("Tag", major, minor, patch))
      case Tag(major, minor, patch) if valid(major, minor, patch) =>
        Right(s"${baseVersion.major}.${baseVersion.minor}.${baseVersion.patch}${postfix}")
      case Detached(commitHash) =>
        Right(s"${baseVersion.major}.${baseVersion.minor}.${baseVersion.patch}.${commitHash}${postfix}-SNAPSHOT")
    }

  }

  def fromConstant(constant: String): () => Success[String] = () => Success(constant)

  sealed trait Reference {}

  sealed case class BaseVersion(major: Int, minor: Int, patch: Int) {
    def bumpMajor(): BaseVersion    = copy(major = major + 1, minor = 0, patch = 0)
    def bumpMinor(): BaseVersion    = copy(minor = minor + 1, patch = 0)
    def bumpPatch(): BaseVersion    = copy(patch = patch + 1)
    override def toString(): String = majorMinorPatch
    def majorMinor: String          = s"$major.$minor"
    def majorMinorPatch: String     = s"$major.$minor.$patch"
  }

  object BaseVersion {
    private def catchNonFatal[A](f: => A): Either[Throwable, A] =
      try {
        Right(f)
      } catch {
        case scala.util.control.NonFatal(t) => Left(t)
      }

    private def versionRegex = """([0-9]+)\.([0-9]+)\.([0-9]+).*""".r

    def parse(s: String): Either[Throwable, BaseVersion] = {
      for {
        regexResult <- versionRegex
                        .findFirstMatchIn(s)
                        .toRight(new RuntimeException("Cannot parse base version for " + s))
        _        <- Either.cond(regexResult.groupCount == 3, (), new RuntimeException("Cannot parse base version for " + s))
        maString <- catchNonFatal(regexResult.group(1)) // this is just paranoia
        miString <- catchNonFatal(regexResult.group(2)) // this is just paranoia
        paString <- catchNonFatal(regexResult.group(3)) // this is just paranoia
        major    <- catchNonFatal(maString.toInt)
        minor    <- catchNonFatal(miString.toInt)
        patch    <- catchNonFatal(paString.toInt)
      } yield {
        BaseVersion(major, minor, patch)
      }
    }
  }

  object References {

    /**
      * Case class modeling the version of a Feature branch
      *
      * @param name The name of the feature branch
      */
    sealed case class Feature private (name: String) extends Reference

    /**
      * Case class modeling the version of a Tag
      *
      * @param major The major version
      * @param minor The minor version
      * @param patch The patch version
      */
    sealed case class Tag private (major: Int, minor: Int, patch: Int) extends Reference

    /**
      * Case class modeling a release branch
      *
      * @param major The major version
      * @param minor The minor version
      */
    sealed case class Release private (major: Int, minor: Int) extends Reference

    /**
      * Case class modeling an Hotfix branch
      *
      * @param name The name of the hotfix branch
      */
    sealed case class Hotfix private (name: String) extends Reference

    /**
      * Case class modeling Develop branch
      */
    case object Develop extends Reference

    /**
      * Case class modeling a Detached commit
      */
    case class Detached(commitHash: String) extends Reference

  }

}
