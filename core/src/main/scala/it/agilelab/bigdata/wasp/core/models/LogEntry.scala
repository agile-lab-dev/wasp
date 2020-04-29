package it.agilelab.bigdata.wasp.core.models

import java.time.Instant

case class LogEntry(log_source: String,
                    log_level: String,
                    message: String,
                    timestamp: Instant,
                    thread: String,
                    cause: Option[String] = None,
                    stacktrace: Option[String] = None)

case class Logs(found: Long, entries: Seq[LogEntry])