package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.core.models.TopicModel
import it.agilelab.bigdata.wasp.whitelabel.models.test._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.GroupState
import org.apache.spark.sql.streaming.GroupStateTimeout
import org.apache.spark.sql.streaming.OutputMode

abstract class TestCheckpointStrategy(val version: String, val topicName: String) extends Strategy {

  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {

    println(s"Strategy version: $version")
    println(s"Strategy configuration: $configuration")
    println(s"Strategy dataFrames received: $dataFrames")

    val dataFrame = dataFrames(ReaderKey(TopicModel.readerType, topicName))
    import dataFrame.sparkSession.implicits._
    import TestCheckpointState.implicits._

    val dataset = dataFrames.head._2.as[TestCheckpointDocument]
    val groupedDataset = dataset.groupByKey(_.id)
    groupedDataset.flatMapGroupsWithState[TestCheckpointState, TestCheckpointDocument](OutputMode.Append, GroupStateTimeout.NoTimeout)(func = update).toDF
  }

  def update: (String, Iterator[TestCheckpointDocument], GroupState[TestCheckpointState]) => Iterator[TestCheckpointDocument]
}

// input JSON test topic format

class TestCheckpointJSONStrategyV1 extends TestCheckpointStrategy("v1", TestTopicModel.jsonCheckpoint.name) {

  override def update = (key: String, iterator: Iterator[TestCheckpointDocument], state: GroupState[TestCheckpointState]) => {
    iterator map { element =>

      val oldState: TestCheckpointState =
        if (!state.exists)
          TestCheckpointStateV1(sum = 0)
        else
          state.get

      state.update(
        oldState match {
          case oldStateImpl: TestCheckpointStateV1 => {
            // do something using oldStateImpl TestCheckpointStateV1
            // ...

            // update state using TestCheckpointStateV1
            TestCheckpointStateV1(sum = oldStateImpl.sum + element.value)
          }
          case oldStateImpl => {
            // do nothing using oldStateImpl
            println(s"ERROR: Ignore State update: State type not managed by strategy version '${version}'")

            // no update state
            oldStateImpl
          }
        }
      )

      println(s"Element: ${element.id} - Version: ${element.version} - Value: ${element.value} - OldState: $oldState - NewState: ${state.get}")

      state.get match {
        case newStateImpl: TestCheckpointStateV1 => {
          // do something using newStateImpl TestCheckpointStateV1
          // ...

          // update dataframe using TestCheckpointStateV1
          element.copy(sum = newStateImpl.sum)
        }
        case newStateImpl => {
          // do nothing using newStateImpl
          println(s"ERROR: Ignore DataFrame update: State type not managed by strategy version '${version}'")

          // no update dataframe
          element
        }
      }
    }
  }
}

/** work on oldstate + mantain oldstate */
class TestCheckpointJSONStrategyV2 extends TestCheckpointStrategy("v2", TestTopicModel.jsonCheckpoint.name) {

  override def update = (key: String, iterator: Iterator[TestCheckpointDocument], state: GroupState[TestCheckpointState]) => {
    iterator map { element =>

      val oldState: TestCheckpointState =
        if (!state.exists)
          TestCheckpointStateV2(sum = 0, oldSumInt = 0)
        else
          state.get

      state.update(
        oldState match {
          case oldStateImpl: TestCheckpointStateV2 => {
            // do something using oldStateImpl TestCheckpointStateV2
            // ...

            // update state using TestCheckpointStateV2
            TestCheckpointStateV2(sum = oldStateImpl.sum + element.value, oldSumInt = oldStateImpl.sum)
          }
          case oldStateImpl: TestCheckpointStateV1 => {
            // do something using oldStateImpl TestCheckpointStateV1
            // ...

            // update state using TestCheckpointStateV1
            TestCheckpointStateV1(sum = oldStateImpl.sum + element.value)
          }
          case oldStateImpl => {
            // do nothing using oldStateImpl
            println(s"ERROR: Ignore State update: State type not managed by strategy version '${version}'")

            // no update state
            oldStateImpl
          }
        }
      )

      println(s"Element: ${element.id} - Version: ${element.version} - Value: ${element.value} - OldState: $oldState - NewState: ${state.get}")

      state.get match {
        case newStateImpl: TestCheckpointStateV2 => {
          // do something using newStateImpl TestCheckpointStateV2
          // ...

          // update dataframe using TestCheckpointStateV2
          element.copy(sum = newStateImpl.sum, oldSumInt = newStateImpl.oldSumInt)
        }
        case newStateImpl: TestCheckpointStateV1 => {
          // do something using newStateImpl TestCheckpointStateV1
          // ...

          // update dataframe using TestCheckpointStateV1
          element.copy(sum = newStateImpl.sum)
        }
        case newStateImpl => {
          // do nothing using newStateImpl
          println(s"ERROR: Ignore DataFrame update: State type not managed by strategy version '${version}'")

          // no update dataframe
          element
        }
      }
    }
  }
}

/** work on oldstate + align oldstate to TestCheckpointStateV3 */
class TestCheckpointJSONStrategyV3 extends TestCheckpointStrategy("v3", TestTopicModel.jsonCheckpoint.name) {

  override def update = (key: String, iterator: Iterator[TestCheckpointDocument], state: GroupState[TestCheckpointState]) => {
    iterator map { element =>

      val oldState: TestCheckpointState =
        if (!state.exists)
          TestCheckpointStateV3(sum = 0, oldSumInt = 0, oldSumString = 0.toString)
        else
          state.get

      state.update(
        oldState match {
          case oldStateImpl: TestCheckpointStateV3 => {
            // do something using oldStateImpl TestCheckpointStateV3
            // ...

            // update state using TestCheckpointStateV3
            TestCheckpointStateV3(sum = oldStateImpl.sum + element.value, oldSumInt = oldStateImpl.sum, oldSumString = oldStateImpl.sum.toString)
          }
          case oldStateImpl: TestCheckpointStateV2 => {
            // do something using oldStateImpl TestCheckpointStateV2
            // ...

            // update state using TestCheckpointStateV3
            TestCheckpointStateV3(sum = oldStateImpl.sum + element.value, oldSumInt = oldStateImpl.sum, oldSumString = oldStateImpl.sum.toString)
          }
          case oldStateImpl: TestCheckpointStateV1 => {
            // do something using oldStateImpl TestCheckpointStateV1
            // ...

            // update state using TestCheckpointStateV3
            TestCheckpointStateV3(sum = oldStateImpl.sum + element.value)
          }
          case oldStateImpl => {
            // do nothing using oldStateImpl
            println(s"ERROR: Ignore State update: State type not managed by strategy version '${version}'")

            // no update state
            oldStateImpl
          }
        }
      )

      println(s"Element: ${element.id} - Version: ${element.version} - Value: ${element.value} - OldState: $oldState - NewState: ${state.get}")

      state.get match {
        case newStateImpl: TestCheckpointStateV3 => {
          // do something using newStateImpl TestCheckpointStateV3
          // ...

          // update dataframe using TestCheckpointStateV3
          element.copy(sum = newStateImpl.sum, oldSumInt = newStateImpl.oldSumInt, oldSumString = newStateImpl.oldSumString)
        }
        case newStateImpl => {
          // do nothing using newStateImpl
          println(s"ERROR: Ignore DataFrame update: State type not managed by strategy version '${version}'")

          // no update dataframe
          element
        }
      }
    }
  }
}

/** work on oldstate + align oldstate to TestCheckpointStateV4 */
class TestCheckpointJSONStrategyV4 extends TestCheckpointStrategy("v4", TestTopicModel.jsonCheckpoint.name) {

  override def update = (key: String, iterator: Iterator[TestCheckpointDocument], state: GroupState[TestCheckpointState]) => {
    iterator map { element =>

      val oldState: TestCheckpointState =
        if (!state.exists)
          TestCheckpointStateV4(sum = 0,
                                stateV4Internal = TestCheckpointStateV4Internal(0, 0.toString))
        else
          state.get

      state.update(
        oldState match {
          case oldStateImpl: TestCheckpointStateV4 => {
            // do something using oldStateImpl TestCheckpointStateV4
            // ...

            // update state using TestCheckpointStateV4
            TestCheckpointStateV4(sum = oldStateImpl.sum + element.value,
                                  stateV4Internal = TestCheckpointStateV4Internal(oldSumInt = oldStateImpl.sum, oldSumString = oldStateImpl.sum.toString))
          }
          case oldStateImpl: TestCheckpointStateV3 => {
            // do something using oldStateImpl TestCheckpointStateV3
            // ...

            // update state using TestCheckpointStateV4
            TestCheckpointStateV4(sum = oldStateImpl.sum + element.value,
                                  stateV4Internal = TestCheckpointStateV4Internal(oldSumInt = oldStateImpl.sum, oldSumString = oldStateImpl.sum.toString))
          }
          case oldStateImpl: TestCheckpointStateV2 => {
            // do something using oldStateImpl TestCheckpointStateV2
            // ...

            // update state using TestCheckpointStateV4
            TestCheckpointStateV4(sum = oldStateImpl.sum + element.value,
                                  stateV4Internal = TestCheckpointStateV4Internal(oldSumInt = oldStateImpl.sum, oldSumString = oldStateImpl.sum.toString))
          }
          case oldStateImpl: TestCheckpointStateV1 => {
            // do something using oldStateImpl TestCheckpointStateV1
            // ...

            // update state using TestCheckpointStateV4
            TestCheckpointStateV4(sum = oldStateImpl.sum + element.value)
          }
          case oldStateImpl => {
            // do nothing using oldStateImpl
            println(s"ERROR: Ignore State update: State type not managed by strategy version '${version}'")

            // no update state
            oldStateImpl
          }
        }
      )

      println(s"Element: ${element.id} - Version: ${element.version} - Value: ${element.value} - OldState: $oldState - NewState: ${state.get}")

      state.get match {
        case newStateImpl: TestCheckpointStateV4 => {
          // do something using newStateImpl TestCheckpointStateV4
          // ...

          // no update dataframe
          element.copy(sum = newStateImpl.sum, oldSumInt = newStateImpl.stateV4Internal.oldSumInt, oldSumString = newStateImpl.stateV4Internal.oldSumString)
        }
        case newStateImpl => {
          // do nothing using newStateImpl
          println(s"ERROR: Ignore DataFrame update: State type not managed by strategy version '${version}'")

          // no update dataframe
          element
        }
      }
    }
  }
}


// input AVRO test topic format

class TestCheckpointAVROStrategyV1 extends TestCheckpointStrategy("v1", TestTopicModel.avroCheckpoint.name) {

  override def update = (key: String, iterator: Iterator[TestCheckpointDocument], state: GroupState[TestCheckpointState]) => {
    iterator map { element =>

      val oldState: TestCheckpointState =
        if (!state.exists)
          TestCheckpointStateV1(sum = 0)
        else
          state.get

      state.update(
        oldState match {
          case oldStateImpl: TestCheckpointStateV1 => {
            // do something using oldStateImpl TestCheckpointStateV1
            // ...

            // update state using TestCheckpointStateV1
            TestCheckpointStateV1(sum = oldStateImpl.sum + element.value)
          }
          case oldStateImpl => {
            // do nothing using oldStateImpl
            println(s"ERROR: Ignore State update: State type not managed by strategy version '${version}'")

            // no update state
            oldStateImpl
          }
        }
      )

      println(s"Element: ${element.id} - Version: ${element.version} - Value: ${element.value} - OldState: $oldState - NewState: ${state.get}")

      state.get match {
        case newStateImpl: TestCheckpointStateV1 => {
          // do something using newStateImpl TestCheckpointStateV1
          // ...

          // update dataframe using TestCheckpointStateV1
          element.copy(sum = newStateImpl.sum)
        }
        case newStateImpl => {
          // do nothing using newStateImpl
          println(s"ERROR: Ignore DataFrame update: State type not managed by strategy version '${version}'")

          // no update dataframe
          element
        }
      }
    }
  }
}

/** work on oldstate + mantain oldstate */
class TestCheckpointAVROStrategyV2 extends TestCheckpointStrategy("v2", TestTopicModel.avroCheckpoint.name) {

  override def update = (key: String, iterator: Iterator[TestCheckpointDocument], state: GroupState[TestCheckpointState]) => {
    iterator map { element =>

      val oldState: TestCheckpointState =
        if (!state.exists)
          TestCheckpointStateV2(sum = 0, oldSumInt = 0)
        else
          state.get

      state.update(
        oldState match {
          case oldStateImpl: TestCheckpointStateV2 => {
            // do something using oldStateImpl TestCheckpointStateV2
            // ...

            // update state using TestCheckpointStateV2
            TestCheckpointStateV2(sum = oldStateImpl.sum + element.value, oldSumInt = oldStateImpl.sum)
          }
          case oldStateImpl: TestCheckpointStateV1 => {
            // do something using oldStateImpl TestCheckpointStateV1
            // ...

            // update state using TestCheckpointStateV1
            TestCheckpointStateV1(sum = oldStateImpl.sum + element.value)
          }
          case oldStateImpl => {
            // do nothing using oldStateImpl
            println(s"ERROR: Ignore State update: State type not managed by strategy version '${version}'")

            // no update state
            oldStateImpl
          }
        }
      )

      println(s"Element: ${element.id} - Version: ${element.version} - Value: ${element.value} - OldState: $oldState - NewState: ${state.get}")

      state.get match {
        case newStateImpl: TestCheckpointStateV2 => {
          // do something using newStateImpl TestCheckpointStateV2
          // ...

          // update dataframe using TestCheckpointStateV2
          element.copy(sum = newStateImpl.sum, oldSumInt = newStateImpl.oldSumInt)
        }
        case newStateImpl: TestCheckpointStateV1 => {
          // do something using newStateImpl TestCheckpointStateV1
          // ...

          // update dataframe using TestCheckpointStateV1
          element.copy(sum = newStateImpl.sum)
        }
        case newStateImpl => {
          // do nothing using newStateImpl
          println(s"ERROR: Ignore DataFrame update: State type not managed by strategy version '${version}'")

          // no update dataframe
          element
        }
      }
    }
  }
}

/** work on oldstate + align oldstate to TestCheckpointStateV3 */
class TestCheckpointAVROStrategyV3 extends TestCheckpointStrategy("v3", TestTopicModel.avroCheckpoint.name) {

  override def update = (key: String, iterator: Iterator[TestCheckpointDocument], state: GroupState[TestCheckpointState]) => {
    iterator map { element =>

      val oldState: TestCheckpointState =
        if (!state.exists)
          TestCheckpointStateV3(sum = 0, oldSumInt = 0, oldSumString = 0.toString)
        else
          state.get

      state.update(
        oldState match {
          case oldStateImpl: TestCheckpointStateV3 => {
            // do something using oldStateImpl TestCheckpointStateV3
            // ...

            // update state using TestCheckpointStateV3
            TestCheckpointStateV3(sum = oldStateImpl.sum + element.value, oldSumInt = oldStateImpl.sum, oldSumString = oldStateImpl.sum.toString)
          }
          case oldStateImpl: TestCheckpointStateV2 => {
            // do something using oldStateImpl TestCheckpointStateV2
            // ...

            // update state using TestCheckpointStateV3
            TestCheckpointStateV3(sum = oldStateImpl.sum + element.value, oldSumInt = oldStateImpl.sum, oldSumString = oldStateImpl.sum.toString)
          }
          case oldStateImpl: TestCheckpointStateV1 => {
            // do something using oldStateImpl TestCheckpointStateV1
            // ...

            // update state using TestCheckpointStateV3
            TestCheckpointStateV3(sum = oldStateImpl.sum + element.value)
          }
          case oldStateImpl => {
            // do nothing using oldStateImpl
            println(s"ERROR: Ignore State update: State type not managed by strategy version '${version}'")

            // no update state
            oldStateImpl
          }
        }
      )

      println(s"Element: ${element.id} - Version: ${element.version} - Value: ${element.value} - OldState: $oldState - NewState: ${state.get}")

      state.get match {
        case newStateImpl: TestCheckpointStateV3 => {
          // do something using newStateImpl TestCheckpointStateV3
          // ...

          // update dataframe using TestCheckpointStateV3
          element.copy(sum = newStateImpl.sum, oldSumInt = newStateImpl.oldSumInt, oldSumString = newStateImpl.oldSumString)
        }
        case newStateImpl => {
          // do nothing using newStateImpl
          println(s"ERROR: Ignore DataFrame update: State type not managed by strategy version '${version}'")

          // no update dataframe
          element
        }
      }
    }
  }
}

/** work on oldstate + align oldstate to TestCheckpointStateV4 */
class TestCheckpointAVROStrategyV4 extends TestCheckpointStrategy("v4", TestTopicModel.avroCheckpoint.name) {

  override def update = (key: String, iterator: Iterator[TestCheckpointDocument], state: GroupState[TestCheckpointState]) => {
    iterator map { element =>

      val oldState: TestCheckpointState =
        if (!state.exists)
          TestCheckpointStateV4(sum = 0,
                                stateV4Internal = TestCheckpointStateV4Internal(0, 0.toString))
        else
          state.get

      state.update(
        oldState match {
          case oldStateImpl: TestCheckpointStateV4 => {
            // do something using oldStateImpl TestCheckpointStateV4
            // ...

            // update state using TestCheckpointStateV4
            TestCheckpointStateV4(sum = oldStateImpl.sum + element.value,
                                  stateV4Internal = TestCheckpointStateV4Internal(oldSumInt = oldStateImpl.sum, oldSumString = oldStateImpl.sum.toString))
          }
          case oldStateImpl: TestCheckpointStateV3 => {
            // do something using oldStateImpl TestCheckpointStateV3
            // ...

            // update state using TestCheckpointStateV4
            TestCheckpointStateV4(sum = oldStateImpl.sum + element.value,
                                  stateV4Internal = TestCheckpointStateV4Internal(oldSumInt = oldStateImpl.sum, oldSumString = oldStateImpl.sum.toString))
          }
          case oldStateImpl: TestCheckpointStateV2 => {
            // do something using oldStateImpl TestCheckpointStateV2
            // ...

            // update state using TestCheckpointStateV4
            TestCheckpointStateV4(sum = oldStateImpl.sum + element.value,
                                  stateV4Internal = TestCheckpointStateV4Internal(oldSumInt = oldStateImpl.sum, oldSumString = oldStateImpl.sum.toString))
          }
          case oldStateImpl: TestCheckpointStateV1 => {
            // do something using oldStateImpl TestCheckpointStateV1
            // ...

            // update state using TestCheckpointStateV4
            TestCheckpointStateV4(sum = oldStateImpl.sum + element.value)
          }
          case oldStateImpl => {
            // do nothing using oldStateImpl
            println(s"ERROR: Ignore State update: State type not managed by strategy version '${version}'")

            // no update state
            oldStateImpl
          }
        }
      )

      println(s"Element: ${element.id} - Version: ${element.version} - Value: ${element.value} - OldState: $oldState - NewState: ${state.get}")

      state.get match {
        case newStateImpl: TestCheckpointStateV4 => {
          // do something using newStateImpl TestCheckpointStateV4
          // ...

          // no update dataframe
          element.copy(sum = newStateImpl.sum, oldSumInt = newStateImpl.stateV4Internal.oldSumInt, oldSumString = newStateImpl.stateV4Internal.oldSumString)
        }
        case newStateImpl => {
          // do nothing using newStateImpl
          println(s"ERROR: Ignore DataFrame update: State type not managed by strategy version '${version}'")

          // no update dataframe
          element
        }
      }
    }
  }
}