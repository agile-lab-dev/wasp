package it.agilelab.bigdata.wasp.db.mongo

import it.agilelab.bigdata.wasp.core.db.WaspDBService
import org.scalatest.{FlatSpec, Matchers}

class WaspMongoDBServiceTest extends FlatSpec with Matchers {

  it should "check that WaspDB use MongoBL" in {
    WaspDBService.service.isInstanceOf[WaspMongoDBService] shouldBe true
  }
}

