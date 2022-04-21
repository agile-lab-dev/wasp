package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils

object CommitStatus {
  sealed trait CommitStatus {
    val status: String
  }
  case object Pending extends CommitStatus {
    override val status: String = "Pending"
  }
  case object Success extends CommitStatus {
    override val status: String = "Success"
  }
  case object Failed extends CommitStatus {
    override val status: String = "Failed"
  }
}
