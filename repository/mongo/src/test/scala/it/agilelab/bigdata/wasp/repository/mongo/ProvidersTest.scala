package it.agilelab.bigdata.wasp.repository.mongo

import com.github.simplyscala.{MongoEmbedDatabase, MongodProps}
import org.scalatest.{BeforeAndAfterAll, Suites, Tag}


object ProvidersTest extends Tag("ProvidersTest")

class ProvidersTest extends Suites(new BatchJobBLImplTest,
                                  new CdcBLImplTest,
                                  new HttpBLImplTest,
                                  new IndexBLImplTest,
                                  new KeyValueBLImplTest,
                                  new PipegraphBLImplTest,
                                  new RawBLImplTest,
                                  new SqlSourceBLImplTest,
                                  new TopicBLImplTest,
                                  new ProcessGroupBLImplTest,
                                  new ProducerBLImplTest,
                                  new BatchSchedulerBLImplTest,
                                  new MlModelBLImplTest,
                                  new WebsocketBLImplTest,
                                  new BatchJobInstanceBLImplTest,
                                  new ConfigManagerBLImplTest) with BeforeAndAfterAll with MongoEmbedDatabase{

  var mongoInstance: MongodProps = null //reference holder variable
  override protected def beforeAll(): Unit = {
    mongoInstance = mongoStart(27017)
  }

  override protected def afterAll(): Unit = {
    mongoStop(mongoInstance)
  }
}