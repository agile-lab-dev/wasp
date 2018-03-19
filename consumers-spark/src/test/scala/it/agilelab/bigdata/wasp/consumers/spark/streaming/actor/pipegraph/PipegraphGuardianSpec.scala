package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph

import akka.actor.{ActorSystem, Props}
import akka.actor.FSM.{CurrentState, SubscribeTransitionCallBack, Transition}
import akka.testkit.{ImplicitSender, TestFSMRef, TestKit, TestProbe}
import it.agilelab.bigdata.wasp.core.models._
import org.scalatest.concurrent.Eventually
import org.scalatest._
import PipegraphGuardian._
import State._
import Data._
import Protocol._
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.Protocol.WorkAvailable
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.{Protocol => MasterProtocol}

import scala.collection.immutable.Map
import scala.concurrent.duration._

class PipegraphGuardianSpec extends TestKit(ActorSystem("WASP"))
                          with WordSpecLike
                          with BeforeAndAfterAll
                          with ImplicitSender
                          with Matchers
                          with Eventually
                          with Inside {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }


  val defaultPipegraph = PipegraphModel(name = "pipegraph",
    description = "",
    owner = "test",
    isSystem = false,
    creationTime = System.currentTimeMillis(),
    legacyStreamingComponents = List.empty,
    structuredStreamingComponents = List.empty,
    rtComponents = List.empty,
    dashboard = None)

  val defaultInstance = PipegraphInstanceModel(name = "pipegraph-1",
    instanceOf = "pipegraph",
    startTimestamp = 1l,
    currentStatusTimestamp = 0l,
    status = PipegraphStatus.PROCESSING)

  "A PipegraphGuardian is in WaitingForWorkState" must {




    "Ask for work when work is available" in {

      val probe = TestProbe()

      val factory: ChildFactory = _ => probe.ref

      val strategy: ComponentFailedStrategy = _ => DontCare

      val fsm = TestFSMRef(new PipegraphGuardian(factory, 1.millisecond, 1.millisecond,strategy))

      probe.send(fsm, SubscribeTransitionCallBack(probe.ref))
      probe.expectMsgType[CurrentState[State]]

      probe.send(fsm, WorkAvailable)

      probe.expectMsg(GimmeWork)






      probe.send(fsm, MasterProtocol.WorkGiven(defaultPipegraph, defaultInstance))


      probe.expectMsg(Transition[State](fsm,WaitingForWork,Activating))



    }

    "Retry if work cannot be given" in {
      val probe = TestProbe()

      val factory: ChildFactory = _ => probe.ref

      val strategy: ComponentFailedStrategy = _ => DontCare

      val fsm = TestFSMRef(new PipegraphGuardian(factory, 1.millisecond, 1.millisecond,strategy))

      probe.send(fsm, SubscribeTransitionCallBack(probe.ref))
      probe.expectMsgType[CurrentState[State]]

      probe.send(fsm, WorkAvailable)

      probe.expectMsg(GimmeWork)

      probe.send(fsm, MasterProtocol.WorkNotGiven(new Exception("Something went wrong")))

      probe.expectMsg(GimmeWork)

      probe.send(fsm, MasterProtocol.WorkNotGiven(new Exception("Something went wrong")))

      probe.expectMsg(GimmeWork)

      probe.send(fsm, MasterProtocol.WorkGiven(defaultPipegraph, defaultInstance))

      probe.expectMsg(Transition[State](fsm,WaitingForWork,Activating))

    }
  }

  "A Pipegraph in Activating State" must {

    "Activate ETL component" in {

      val probe = TestProbe()

      val factory: ChildFactory = _ => probe.ref

      val strategy: ComponentFailedStrategy = _ => DontCare

      val fsm = TestFSMRef(new PipegraphGuardian(factory, 1.millisecond, 1.millisecond,strategy))

      probe.send(fsm, SubscribeTransitionCallBack(probe.ref))
      probe.expectMsgType[CurrentState[State]]

      probe.send(fsm, WorkAvailable)

      probe.expectMsg(GimmeWork)

      val testStructuredModel = StructuredStreamingETLModel(name = "component",
        inputs = List(ReaderModel.kafkaReader("", "")),
        output = WriterModel.solrWriter("",""),
        mlModels = List(),
        strategy = None,
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
        config = Map())

      val pipegraph = defaultPipegraph.copy(structuredStreamingComponents = List(testStructuredModel))

      probe.send(fsm, MasterProtocol.WorkGiven(pipegraph, defaultInstance))

      probe.expectMsg(Transition[State](fsm,WaitingForWork,Activating))

      probe.expectMsg(ActivateETL(testStructuredModel))

      probe.send(fsm, ETLActivated)

      probe.expectMsg(Transition[State](fsm,Activating,Activating))

      probe.expectMsg(Transition[State](fsm,Activating,Materializing))


    }

    "Honor dont care strategy" in {

      val probe = TestProbe()

      val factory: ChildFactory = _ => probe.ref

      val strategy: ComponentFailedStrategy = _ => DontCare

      val fsm = TestFSMRef(new PipegraphGuardian(factory, 1.millisecond, 1.millisecond,strategy))

      probe.send(fsm, SubscribeTransitionCallBack(probe.ref))
      probe.expectMsgType[CurrentState[State]]

      probe.send(fsm, WorkAvailable)

      probe.expectMsg(GimmeWork)

      val testStructuredModel = StructuredStreamingETLModel(name = "component",
        inputs = List(ReaderModel.kafkaReader("", "")),
        output = WriterModel.solrWriter("",""),
        mlModels = List(),
        strategy = None,
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
        config = Map())

      val pipegraph = defaultPipegraph.copy(structuredStreamingComponents = List(testStructuredModel))

      probe.send(fsm, MasterProtocol.WorkGiven(pipegraph, defaultInstance))

      probe.expectMsg(Transition[State](fsm,WaitingForWork,Activating))

      probe.expectMsg(ActivateETL(testStructuredModel))

      probe.send(fsm, ETLNotActivated)

      probe.expectMsg(Transition[State](fsm,Activating,Activating))
      probe.expectMsg(Transition[State](fsm,Activating,Materializing))


    }

    "Honor retry strategy" in {

      val probe = TestProbe()

      val childProbe = TestProbe()
      val factory: ChildFactory = _ => childProbe.ref

      val strategy: ComponentFailedStrategy = _ => Retry

      val fsm = TestFSMRef(new PipegraphGuardian(factory, 1.millisecond, 1.millisecond,strategy))

      probe.send(fsm, SubscribeTransitionCallBack(probe.ref))
      probe.expectMsgType[CurrentState[State]]

      probe.send(fsm, WorkAvailable)

      probe.expectMsg(GimmeWork)

      val testStructuredModel = StructuredStreamingETLModel(name = "component",
        inputs = List(ReaderModel.kafkaReader("", "")),
        output = WriterModel.solrWriter("",""),
        mlModels = List(),
        strategy = None,
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
        config = Map())

      val pipegraph = defaultPipegraph.copy(structuredStreamingComponents = List(testStructuredModel))

      probe.send(fsm, MasterProtocol.WorkGiven(pipegraph, defaultInstance))

      probe.expectMsg(Transition[State](fsm,WaitingForWork,Activating))

      childProbe.expectMsg(ActivateETL(testStructuredModel))

      childProbe.send(fsm, ETLNotActivated)

      childProbe.expectMsg(ActivateETL(testStructuredModel))

      childProbe.send(fsm, ETLActivated)


      probe.expectMsg(Transition[State](fsm,Activating,Activating))
      probe.expectMsg(Transition[State](fsm,Activating,Activating))
      probe.expectMsg(Transition[State](fsm,Activating,Materializing))

    }

    "Honor StopAll strategy" in {

      val probe = TestProbe()

      val probes = Seq(TestProbe("child-probe-1"),TestProbe("child-probe-2"))
      val probesIterator = probes.iterator
      val factory: ChildFactory = _ => probesIterator.next().ref


      val testStructuredModel = StructuredStreamingETLModel(name = "component",
        inputs = List(ReaderModel.kafkaReader("", "")),
        output = WriterModel.solrWriter("",""),
        mlModels = List(),
        strategy = None,
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
        config = Map())

      val testStructuredModelStopAll = StructuredStreamingETLModel(name = "componentStopAll",
        inputs = List(ReaderModel.kafkaReader("", "")),
        output = WriterModel.solrWriter("",""),
        mlModels = List(),
        strategy = None,
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
        config = Map())

      val strategy: ComponentFailedStrategy = {
        case `testStructuredModel` => DontCare
        case `testStructuredModelStopAll` => StopAll
      }

      val fsm = TestFSMRef(new PipegraphGuardian(factory, 1.millisecond, 1.millisecond,strategy))

      probe.send(fsm, SubscribeTransitionCallBack(probe.ref))
      probe.expectMsgType[CurrentState[State]]

      probe.send(fsm, WorkAvailable)

      probe.expectMsg(GimmeWork)


      val pipegraph = defaultPipegraph.copy(structuredStreamingComponents = List(testStructuredModel,
        testStructuredModelStopAll))

      probe.send(fsm, MasterProtocol.WorkGiven(pipegraph, defaultInstance))

      probe.expectMsg(Transition[State](fsm,WaitingForWork,Activating))

      probes(0).expectMsg(ActivateETL(testStructuredModel))
      probes(0).send(fsm, ETLActivated)
      probe.expectMsg(Transition[State](fsm,Activating,Activating))

      probes(1).expectMsg(ActivateETL(testStructuredModelStopAll))
      probes(1).send(fsm, ETLNotActivated)
      probe.expectMsg(Transition[State](fsm,Activating,Activating))

      probe.expectMsg(Transition[State](fsm,Activating,Stopping))


    }

  }

  "A PipegraphGuardian in Materializing State" must {
    "Materialize ETLs" in {

      val probe = TestProbe()

      val probes = Seq(TestProbe("child-probe-1"),TestProbe("child-probe-2"))
      val probesIterator = probes.iterator
      val factory: ChildFactory = _ => probesIterator.next().ref


      val testStructuredModel = StructuredStreamingETLModel(name = "component",
        inputs = List(ReaderModel.kafkaReader("", "")),
        output = WriterModel.solrWriter("",""),
        mlModels = List(),
        strategy = None,
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
        config = Map())

      val testStructuredModelStopAll = StructuredStreamingETLModel(name = "component",
        inputs = List(ReaderModel.kafkaReader("", "")),
        output = WriterModel.solrWriter("",""),
        mlModels = List(),
        strategy = None,
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
        config = Map())

      val strategy: ComponentFailedStrategy = {
        case `testStructuredModel` => DontCare
        case `testStructuredModelStopAll` => StopAll
      }

      val fsm = TestFSMRef(new PipegraphGuardian(factory, 1.millisecond, 1.millisecond,strategy))

      probe.send(fsm, SubscribeTransitionCallBack(probe.ref))
      probe.expectMsgType[CurrentState[State]]

      probe.send(fsm, WorkAvailable)

      probe.expectMsg(GimmeWork)


      val pipegraph = defaultPipegraph.copy(structuredStreamingComponents = List(testStructuredModel,
        testStructuredModelStopAll))

      probe.send(fsm, MasterProtocol.WorkGiven(pipegraph, defaultInstance))

      probe.expectMsg(Transition[State](fsm,WaitingForWork,Activating))

      probes(0).expectMsg(ActivateETL(testStructuredModel))
      probes(0).send(fsm, ETLActivated)

      probe.expectMsg(Transition[State](fsm,Activating,Activating))

      probes(1).expectMsg(ActivateETL(testStructuredModelStopAll))
      probes(1).send(fsm, ETLActivated)


      probe.expectMsg(Transition[State](fsm,Activating,Activating))
      probe.expectMsg(Transition[State](fsm,Activating,Materializing))


      probes(0).expectMsg(MaterializeETL)
      probes(1).expectMsg(MaterializeETL)


      probes(0).send(fsm, ETLMaterialized)
      probes(1).send(fsm, ETLMaterialized)

      probe.expectMsg(Transition[State](fsm,Materializing,Monitoring))

    }
  }

  "A PipegraphGuardian in Monitoring State" must {
    "Honor Retry strategy" in {

      val probe = TestProbe()

      val probes = Seq(TestProbe("child-probe-1"),TestProbe("child-probe-2"),TestProbe("child-probe-3"))
      val probesIterator = probes.iterator
      val factory: ChildFactory = _ => probesIterator.next().ref


      val testStructuredModel = StructuredStreamingETLModel(name = "component",
        inputs = List(ReaderModel.kafkaReader("", "")),
        output = WriterModel.solrWriter("",""),
        mlModels = List(),
        strategy = None,
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
        config = Map())

      val testStructuredModelRetry = StructuredStreamingETLModel(name = "retry",
        inputs = List(ReaderModel.kafkaReader("", "")),
        output = WriterModel.solrWriter("",""),
        mlModels = List(),
        strategy = None,
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
        config = Map())

      val strategy: ComponentFailedStrategy = {
        case `testStructuredModel` => DontCare
        case `testStructuredModelRetry` => Retry
      }

      val fsm = TestFSMRef(new PipegraphGuardian(factory, 1.millisecond, 1.millisecond,strategy))

      probe.send(fsm, SubscribeTransitionCallBack(probe.ref))
      probe.expectMsgType[CurrentState[State]]

      probe.send(fsm, WorkAvailable)

      probe.expectMsg(GimmeWork)


      val pipegraph = defaultPipegraph.copy(structuredStreamingComponents = List(testStructuredModel,
        testStructuredModelRetry))

      probe.send(fsm, MasterProtocol.WorkGiven(pipegraph, defaultInstance))

      probe.expectMsg(Transition[State](fsm,WaitingForWork,Activating))

      probes(0).expectMsg(ActivateETL(testStructuredModel))
      probes(0).send(fsm, ETLActivated)

      probe.expectMsg(Transition[State](fsm,Activating,Activating))

      probes(1).expectMsg(ActivateETL(testStructuredModelRetry))
      probes(1).send(fsm, ETLActivated)


      probe.expectMsg(Transition[State](fsm,Activating,Activating))
      probe.expectMsg(Transition[State](fsm,Activating,Materializing))


      probes(0).expectMsg(MaterializeETL)
      probes(1).expectMsg(MaterializeETL)


      probes(0).send(fsm, ETLMaterialized)
      probes(1).send(fsm, ETLMaterialized)

      probe.expectMsg(Transition[State](fsm,Materializing,Monitoring))

      probes(1).send(fsm, MasterProtocol.ETLStatusFailed(new Exception("Failed")))

      probe.expectMsg(Transition[State](fsm,Monitoring, Activating))

      probes(2).expectMsg(ActivateETL(testStructuredModelRetry))
      probes(2).send(fsm, ETLActivated)

      probe.expectMsg(Transition[State](fsm,Activating,Activating))

      probe.expectMsg(Transition[State](fsm,Activating,Materializing))

      probes(2).expectMsg(MaterializeETL)
      probes(2).send(fsm, ETLMaterialized)

      probe.expectMsg(Transition[State](fsm,Materializing,Monitoring))



      inside(fsm.stateData) {
        case MonitoringData(_,_, monitoring, _) =>
          monitoring should be(Map(probes(2).ref -> testStructuredModelRetry, probes(0).ref -> testStructuredModel))
      }

      println(prettyPrint(fsm.stateData))

      probes(0).expectNoMsg(1.millisecond)
      probes(1).expectNoMsg(1.millisecond)
    }
  }



  private def prettyPrint(a: Any, indentSize: Int = 2, maxElementWidth: Int = 30, depth: Int = 0): String = {
    val indent = " " * depth * indentSize
    val fieldIndent = indent + (" " * indentSize)
    val thisDepth = prettyPrint(_: Any, indentSize, maxElementWidth, depth)
    val nextDepth = prettyPrint(_: Any, indentSize, maxElementWidth, depth + 1)
    a match {
      // Make Strings look similar to their literal form.
      case s: String =>
        val replaceMap = Seq(
          "\n" -> "\\n",
          "\r" -> "\\r",
          "\t" -> "\\t",
          "\"" -> "\\\""
        )
        '"' + replaceMap.foldLeft(s) { case (acc, (c, r)) => acc.replace(c, r) } + '"'
      // For an empty Seq just use its normal String representation.
      case xs: Seq[_] if xs.isEmpty => xs.toString()
      case xs: Seq[_] =>
        // If the Seq is not too long, pretty print on one line.
        val resultOneLine = xs.map(nextDepth).toString()
        if (resultOneLine.length <= maxElementWidth) return resultOneLine
        // Otherwise, build it with newlines and proper field indents.
        val result = xs.map(x => s"\n$fieldIndent${nextDepth(x)}").toString()
        result.substring(0, result.length - 1) + "\n" + indent + ")"
      // Product should cover case classes.
      case p: Product =>
        val prefix = p.productPrefix
        // We'll use reflection to get the constructor arg names and values.
        val cls = p.getClass
        val fields = cls.getDeclaredFields.filterNot(_.isSynthetic).map(_.getName)
        val values = p.productIterator.toSeq
        // If we weren't able to match up fields/values, fall back to toString.
        if (fields.length != values.length) return p.toString
        fields.zip(values).toList match {
          // If there are no fields, just use the normal String representation.
          case Nil => p.toString
          // If there is just one field, let's just print it as a wrapper.
          case (_, value) :: Nil => s"$prefix(${thisDepth(value)})"
          // If there is more than one field, build up the field names and values.
          case kvps =>
            val prettyFields = kvps.map { case (k, v) => s"$fieldIndent$k = ${nextDepth(v)}" }
            // If the result is not too long, pretty print on one line.
            val resultOneLine = s"$prefix(${prettyFields.mkString(", ")})"
            if (resultOneLine.length <= maxElementWidth) return resultOneLine
            // Otherwise, build it with newlines and proper field indents.
            s"$prefix(\n${prettyFields.mkString(",\n")}\n$indent)"
        }
      // If we haven't specialized this type, just use its toString.
      case _ => a.toString
    }
  }
}