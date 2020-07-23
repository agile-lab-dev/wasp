package it.agilelab.bigdata.wasp.repository.postgres.tables

import java.sql.ResultSet

import it.agilelab.bigdata.wasp.repository.postgres.utils.PostgresSuite

class packageTest extends PostgresSuite{

  it should "test" in {
    val connection = getConnection
    connection.prepareStatement("""CREATE TABLE "TABLE_TEST"( id integer, test varchar, city varchar)""").execute()
    connection.prepareStatement("""INSERT INTO "TABLE_TEST"(id,test,city) VALUES(1,'tester',null)""").execute()
    val rs = connection.prepareStatement("""SELECT * FROM "TABLE_TEST"""").executeQuery()
    rs.next()
    rs.getOption[Int]("id").get shouldBe 1
    rs.getOption[String]("test").get shouldBe "tester"
    an [ClassCastException] should be thrownBy rs.getOption[Int]("test").get
    rs.getOption[String]("city") shouldBe None

  }

}
