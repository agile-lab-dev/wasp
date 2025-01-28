package it.agilelab.bigdata.wasp.consumers.spark.plugins.postgresql

import java.sql.Timestamp

case class TestData(pk1: String, pk2: Int, val1: String, val2: Long, val3: Timestamp)
