package it.agilelab.bigdata.wasp.repository.postgres

import it.agilelab.bigdata.wasp.repository.postgres.bl._
import it.agilelab.bigdata.wasp.repository.postgres.tables.packageTest
import it.agilelab.bigdata.wasp.repository.postgres.utils.PostgresSuite

class AllPostgresTests
    extends PostgresSuite
    with BatchJobBLImplTest
    with BatchJobInstanceBLImplTest
    with BatchSchedulersBLImplTest
    with ConfigManagerBLImplTest
    with DocumentBLImplTest
    with FreeCodeBLImplTest
    with GenericBLImplTest
    with HttpBLImplTest
    with IndexBLImplTest
    with KeyValueBLImplTest
    with MlModelBLImplTest
    with PipegraphBLImplTest
    with PipegraphInstanceBLImplTest
    with ProcessGroupBLImplTest
    with ProducerBLImplTest
    with RawBLImplTest
    with SqlSourceBlImplTest
    with TopicBLImplTest
    with WebsocketBLImplTest
    with packageTest
