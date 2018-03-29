package it.agilelab.bigdata.wasp.whitelabel.models.test

object TestCheckpointState {
  object implicits {
    implicit val testCheckpointStateEncoder = org.apache.spark.sql.Encoders.kryo[TestCheckpointState]
  }
}

class TestCheckpointState


case class TestCheckpointStateV1(sum: Int) extends TestCheckpointState


case class TestCheckpointStateV2(sum: Int,
                                 oldSumInt: Int = -1) extends TestCheckpointState


case class TestCheckpointStateV3(sum: Int,
                                 oldSumInt: Int = -1,
                                 oldSumString: String = "-1") extends TestCheckpointState


case class TestCheckpointStateV4(sum: Int,
                                 stateV4Internal: TestCheckpointStateV4Internal = TestCheckpointStateV4Internal()) extends TestCheckpointState

case class TestCheckpointStateV4Internal(oldSumInt: Int = -1,
                                         oldSumString: String = "-1")