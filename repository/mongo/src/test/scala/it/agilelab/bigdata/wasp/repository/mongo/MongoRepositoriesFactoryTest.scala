package it.agilelab.bigdata.wasp.repository.mongo

import it.agilelab.bigdata.wasp.repository.core.db.RepositoriesFactory
import org.scalatest.{FlatSpec, Matchers}

class MongoRepositoriesFactoryTest extends FlatSpec with Matchers {

  it should "check that WaspDB use MongoBL" in {
    RepositoriesFactory.service.isInstanceOf[MongoRepositoriesFactory] shouldBe true
  }
}

