package it.agilelab.bigdata.wasp.consumers.spark.batch

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRef, ActorRefFactory, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.messages.BatchMessages
import it.agilelab.bigdata.wasp.core.models._
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}

import scala.collection.mutable.ListBuffer


class MockBatchInstancesBl extends BatchJobInstanceBL {

  val buffer: ListBuffer[BatchJobInstanceModel] = ListBuffer()

  override def insert(instance: BatchJobInstanceModel): BatchJobInstanceModel = {
    buffer += instance
    instance
  }

  override def update(instance: BatchJobInstanceModel): BatchJobInstanceModel = {
    buffer.remove(buffer.toIndexedSeq.indexWhere(_.name == instance.name))
    buffer += instance
    instance
  }

  override def all(): Seq[BatchJobInstanceModel] = buffer

  override def instancesOf(name: String): Seq[BatchJobInstanceModel] = buffer.filter(_.instanceOf == name)
}

class MockBatchBl(batchJobInstanceBL: BatchJobInstanceBL) extends BatchJobBL {

  val buffer: ListBuffer[BatchJobModel] = ListBuffer()


  override def getByName(name: String): Option[BatchJobModel] = buffer.find(_.name == name)

  override def getAll: Seq[BatchJobModel] = buffer

  override def update(batchJobModel: BatchJobModel): Unit = {
    buffer.remove(buffer.toIndexedSeq.indexWhere(_.name == batchJobModel.name))
    buffer += batchJobModel
  }

  override def insert(batchJobModel: BatchJobModel): Unit = {
    buffer += batchJobModel
  }

  override def deleteByName(name: String): Unit = buffer.remove(buffer.toIndexedSeq.indexWhere(_.name == name))


  override def instances(): BatchJobInstanceBL = batchJobInstanceBL
}

class MockBatchSchedulersBl extends BatchSchedulersBL {
  val buffer: ListBuffer[BatchSchedulerModel] = ListBuffer()


  override def getActiveSchedulers(isActive: Boolean): Seq[BatchSchedulerModel] = buffer.filter(_.isActive == isActive)

  override def persist(schedulerModel: BatchSchedulerModel): Unit = buffer += schedulerModel
}

class SparkConsumersBatchMasterGuardianSpec
  extends TestKit(ActorSystem("WASP"))
    with WordSpecLike
    with BeforeAndAfterAll
    with ImplicitSender {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A SparkConsumersBatchMasterGuardian" must {

    "Reset all PROCESSING jobs to PENDING when starting" in {

      val mockBl = new MockBatchBl(new MockBatchInstancesBl)
      mockBl.instances().insert(BatchJobInstanceModel("job1-1", "job1", 1l, 0l, JobStatus.PENDING))
      mockBl.instances().insert(BatchJobInstanceModel("job1-2", "job1", 1l, 0l, JobStatus.PROCESSING))
      val schedulersBL = new MockBatchSchedulersBl

      val master = system.actorOf(SparkConsumersBatchMasterGuardian.props(mockBl,schedulersBL, 5, _ => testActor))

      watch(master)

      master ! SparkConsumersBatchMasterGuardian.Terminate

      expectTerminated(master)


      val instances = mockBl.instances().all()

      assert(instances.exists { instance => instance.name == "job1-1" && instance.status == JobStatus.PENDING })
      assert(instances.exists { instance => instance.name == "job1-2" && instance.status == JobStatus.PENDING })


    }


    "Start slave actors" in {

      val mockBl = new MockBatchBl(new MockBatchInstancesBl)
      val schedulersBL = new MockBatchSchedulersBl
      mockBl.instances().insert(BatchJobInstanceModel("job1-1", "job1", 1l, 0l, JobStatus.PENDING))
      mockBl.instances().insert(BatchJobInstanceModel("job1-2", "job1", 1l, 0l, JobStatus.PENDING))

      val counter = new AtomicInteger()

      val factory: ActorRefFactory => ActorRef = _ => {
        counter.incrementAndGet()
        testActor
      }

      val master = system.actorOf(SparkConsumersBatchMasterGuardian.props(mockBl,schedulersBL, 5, factory))

      watch(master)

      master ! SparkConsumersBatchMasterGuardian.Terminate

      expectTerminated(master)

      assertResult(5)(counter.get())

    }


    "Respond success on StartJob when job has moved to Pending" in {

      val mockBl = new MockBatchBl(new MockBatchInstancesBl)
      val schedulersBL = new MockBatchSchedulersBl

      val job = BatchJobModel(name = "job",
        description = "testJob",
        owner = "test",
        system = false,
        creationTime = System.currentTimeMillis(),
        etl = BatchETLModel(
          name ="name",
          inputs = List.empty,
          output = WriterModel.kafkaWriter("test", "test"),
          mlModels = List.empty,
          strategy = None,
          kafkaAccessType = ""
        ))

      mockBl.insert(job)
      mockBl.instances().insert(BatchJobInstanceModel("job1-1", "job1", 1l, 0l, JobStatus.PENDING))
      mockBl.instances().insert(BatchJobInstanceModel("job1-2", "job1", 1l, 0l, JobStatus.PENDING))


      val factory: ActorRefFactory => ActorRef = _ => testActor

      val master = system.actorOf(SparkConsumersBatchMasterGuardian.props(mockBl,schedulersBL, 5, factory))

      val restConfig = ConfigFactory.parseString("""stringKey = "stringValue", intKey = 1""")

      master ! BatchMessages.StartBatchJob("job", restConfig)

      expectMsg(BatchMessages.StartBatchJobResultSuccess("job"))

      watch(master)

      master ! SparkConsumersBatchMasterGuardian.Terminate

      expectTerminated(master)

      val instances = mockBl.instances().all()

      assert(instances.exists { instance => instance.name == "job1-1" && instance.status == JobStatus.PENDING })
      assert(instances.exists { instance => instance.name == "job1-2" && instance.status == JobStatus.PENDING })
      assert(instances.exists { instance => instance.instanceOf == "job" && instance.status == JobStatus.PENDING && instance.restConfig == restConfig })


    }


    "Respond failure on StartJob when job cannot move to Pending" in {

      val mockBl = new MockBatchBl(new MockBatchInstancesBl {
        override def insert(instance: BatchJobInstanceModel): BatchJobInstanceModel = throw new RuntimeException("Sorry, database is unavailable")
      })

      val schedulersBL = new MockBatchSchedulersBl

      val job = BatchJobModel(name = "job",
        description = "testJob",
        owner = "test",
        system = false,
        creationTime = System.currentTimeMillis(),
        etl = BatchETLModel(
          name ="name",
          inputs = List.empty,
          output = WriterModel.kafkaWriter("test", "test"),
          mlModels = List.empty,
          strategy = None,
          kafkaAccessType = ""
        ))

      mockBl.insert(job)

      val factory: ActorRefFactory => ActorRef = _ => testActor


      val master = system.actorOf(SparkConsumersBatchMasterGuardian.props(mockBl,schedulersBL, 5, factory))

      master ! BatchMessages.StartBatchJob("job", ConfigFactory.empty)

      expectMsg(BatchMessages.StartBatchJobResultFailure("job", "failure creating new batch job instance [Sorry, database is unavailable]"))

      watch(master)

      master ! SparkConsumersBatchMasterGuardian.Terminate

      expectTerminated(master)


    }

    "Respond failure on StartJob when instance of job is already PENDING or PROCESSING" in {

      val mockBl = new MockBatchBl(new MockBatchInstancesBl)
      val schedulersBL = new MockBatchSchedulersBl

      val job1 = BatchJobInstanceModel("job-1", "job", 1l, 0l, JobStatus.PENDING)

      val factory: ActorRefFactory => ActorRef = _ => testActor

      mockBl.instances().insert(job1)

      val master = system.actorOf(SparkConsumersBatchMasterGuardian.props(mockBl,schedulersBL, 5, factory))

      master ! BatchMessages.StartBatchJob("job", ConfigFactory.empty)

      expectMsg(BatchMessages.StartBatchJobResultFailure("job", "Cannot start multiple instances of same job [job]"))

      watch(master)

      master ! SparkConsumersBatchMasterGuardian.Terminate

      expectTerminated(master)


    }

    "Respond failure on StopJob when instance of job is not PENDING or PROCESSING" in {

      val mockBl = new MockBatchBl(new MockBatchInstancesBl)
      val schedulersBL = new MockBatchSchedulersBl

      val job1 = BatchJobInstanceModel("job-1", "job", 1l, 0l, JobStatus.FAILED)

      mockBl.instances().insert(job1)

      val factory: ActorRefFactory => ActorRef = _ => testActor


      val master = system.actorOf(SparkConsumersBatchMasterGuardian.props(mockBl,schedulersBL, 5, factory))

      master ! BatchMessages.StopBatchJob("job")

      expectMsg(BatchMessages.StopBatchJobResultFailure("job", "Cannot stop job [job] whose instances are not running or pending"))

      watch(master)

      master ! SparkConsumersBatchMasterGuardian.Terminate

      expectTerminated(master)


    }

    "Respond failure on StopJob when job cannot move to STOPPED" in {

      val mockBl = new MockBatchBl(new MockBatchInstancesBl {
        override def update(instance: BatchJobInstanceModel): BatchJobInstanceModel = {
          if(instance.status!=JobStatus.PENDING) {
            throw new RuntimeException("Sorry, database is unavailable")
          }else{
            instance
          }
        }
      })

      val schedulersBL = new MockBatchSchedulersBl

      val job = BatchJobModel(name = "job",
        description = "testJob",
        owner = "test",
        system = false,
        creationTime = System.currentTimeMillis(),
        etl = BatchETLModel(
          name ="name",
          inputs = List.empty,
          output = WriterModel.kafkaWriter("test", "test"),
          mlModels = List.empty,
          strategy = None,
          kafkaAccessType = ""
        ))

      mockBl.insert(job)

      val job1 = BatchJobInstanceModel("job1-1", "job", 1l, 0l, JobStatus.PENDING)

      mockBl.instances().insert(job1)

      val factory: ActorRefFactory => ActorRef = _ => testActor

      val master = system.actorOf(SparkConsumersBatchMasterGuardian.props(mockBl,schedulersBL, 5, factory))

      master ! BatchMessages.StopBatchJob("job")

      expectMsg(BatchMessages.StopBatchJobResultFailure("job", "failure stopping instances of job [Sorry, database is unavailable]"))

      watch(master)

      master ! SparkConsumersBatchMasterGuardian.Terminate

      expectTerminated(master)


    }

    "Respond success on StopJob when job has moved to STOPPED" in {

      val mockBl = new MockBatchBl(new MockBatchInstancesBl)
      val schedulersBL = new MockBatchSchedulersBl

      val job = BatchJobModel(name = "job",
        description = "testJob",
        owner = "test",
        system = false,
        creationTime = System.currentTimeMillis(),
        etl = BatchETLModel(
          name ="name",
          inputs = List.empty,
          output = WriterModel.kafkaWriter("test", "test"),
          mlModels = List.empty,
          strategy = None,
          kafkaAccessType = ""
        ))

      mockBl.insert(job)


      val job1 = BatchJobInstanceModel("job-1", "job", 1l, 0l, JobStatus.PENDING)

      mockBl.instances().insert(job1)

      val factory: ActorRefFactory => ActorRef = _ => testActor


      val master = system.actorOf(SparkConsumersBatchMasterGuardian.props(mockBl,schedulersBL, 5, factory))

      master ! BatchMessages.StopBatchJob("job")

      expectMsg(BatchMessages.StopBatchJobResultSuccess("job"))

      watch(master)

      master ! SparkConsumersBatchMasterGuardian.Terminate

      expectTerminated(master)

      assert(mockBl.instances().instancesOf("job").exists(_.status==JobStatus.STOPPED))


    }


    "Give on job to Actors asking for one if job is available" in {

      val mockBl = new MockBatchBl(new MockBatchInstancesBl)
      val schedulersBL = new MockBatchSchedulersBl

      val job = BatchJobModel(name = "job",
        description = "testJob",
        owner = "test",
        system = false,
        creationTime = System.currentTimeMillis(),
        etl = BatchETLModel(
          name ="name",
          inputs = List.empty,
          output = WriterModel.kafkaWriter("test", "test"),
          mlModels = List.empty,
          strategy = None,
          kafkaAccessType = ""
        ))


      mockBl.insert(job)

      val restConfig = ConfigFactory.parseString("""stringKey = "stringValue", intKey = 1""")

      val pendingJobInstance = BatchJobInstanceModel("job-1", "job", 1l, 0l, JobStatus.PENDING, restConfig)


      mockBl.instances().insert(pendingJobInstance)


      val probe = TestProbe()

      val factory: ActorRefFactory => ActorRef = _ => probe.ref

      val master = system.actorOf(SparkConsumersBatchMasterGuardian.props(mockBl,schedulersBL, 5, factory))

      probe.send(master, SparkConsumersBatchMasterGuardian.GimmeOneJob)

      val message = probe.expectMsgClass(classOf[SparkConsumersBatchMasterGuardian.Job])


      assert(message.model == job)
      assert(message.instance.instanceOf == job.name)
      assert(message.instance.restConfig == restConfig)


      watch(master)

      master ! SparkConsumersBatchMasterGuardian.Terminate

      expectTerminated(master)
    }


    "Respond no job available to Actors asking for one when no job is pending" in {

      val mockBl = new MockBatchBl(new MockBatchInstancesBl)
      val schedulersBL = new MockBatchSchedulersBl

      val job = BatchJobModel(name = "job",
        description = "testJob",
        owner = "test",
        system = false,
        creationTime = System.currentTimeMillis(),
        etl = BatchETLModel(
          name ="name",
          inputs = List.empty,
          output = WriterModel.kafkaWriter("test", "test"),
          mlModels = List.empty,
          strategy = None,
          kafkaAccessType = ""
        ))

      mockBl.insert(job)


      val probe = TestProbe()

      val factory: ActorRefFactory => ActorRef = _ => probe.ref

      val master = system.actorOf(SparkConsumersBatchMasterGuardian.props(mockBl,schedulersBL, 5, factory))

      probe.send(master, SparkConsumersBatchMasterGuardian.GimmeOneJob)

      probe.expectMsg(SparkConsumersBatchMasterGuardian.NoJobsAvailable)

      watch(master)

      master ! SparkConsumersBatchMasterGuardian.Terminate

      expectTerminated(master)
    }


    "Update job statuses when receiving Success Response from slave actors" in {

      val mockBl = new MockBatchBl(new MockBatchInstancesBl)
      val schedulersBL = new MockBatchSchedulersBl

      val job = BatchJobModel(name = "job",
        description = "testJob",
        owner = "test",
        system = false,
        creationTime = System.currentTimeMillis(),
        etl = BatchETLModel(
          name ="name",
          inputs = List.empty,
          output = WriterModel.kafkaWriter("test", "test"),
          mlModels = List.empty,
          strategy = None,
          kafkaAccessType = ""
        ))

      mockBl.insert(job)


      val jobInstance = BatchJobInstanceModel("job-1", "job", 1l, 0l, JobStatus.PENDING)


      mockBl.instances().insert(jobInstance)

      val probe = TestProbe()

      val factory: ActorRefFactory => ActorRef = _ => probe.ref



      val master = system.actorOf(SparkConsumersBatchMasterGuardian.props(mockBl,schedulersBL, 5, factory))

      probe.send(master, SparkConsumersBatchMasterGuardian.GimmeOneJob)

      val jobMessage = probe.expectMsgClass(classOf[SparkConsumersBatchMasterGuardian.Job])

      probe.send(master, SparkConsumersBatchMasterGuardian.JobSucceeded(jobMessage.model, jobMessage.instance))
      probe.expectMsg("OK")


      assertResult(JobStatus.SUCCESSFUL)(mockBl.instances().instancesOf("job").head.status)

      watch(master)

      master ! SparkConsumersBatchMasterGuardian.Terminate

      expectTerminated(master)


    }

    "Update job statuses when receiving Failure Response from slave actors" in {

      val mockBl = new MockBatchBl(new MockBatchInstancesBl)
      val schedulersBL = new MockBatchSchedulersBl

      val job = BatchJobModel(name = "job",
        description = "testJob",
        owner = "test",
        system = false,
        creationTime = System.currentTimeMillis(),
        etl = BatchETLModel(
          name ="name",
          inputs = List.empty,
          output = WriterModel.kafkaWriter("test", "test"),
          mlModels = List.empty,
          strategy = None,
          kafkaAccessType = ""
        ))

      mockBl.insert(job)


      val jobInstance = BatchJobInstanceModel("job-1", "job", 1l, 0l, JobStatus.PENDING)

      mockBl.instances().insert(jobInstance)

      val probe = TestProbe()

      val factory: ActorRefFactory => ActorRef = _ => probe.ref


      val master = system.actorOf(SparkConsumersBatchMasterGuardian.props(mockBl,schedulersBL, 5, factory))

      probe.send(master, SparkConsumersBatchMasterGuardian.GimmeOneJob)

      val jobMessage = probe.expectMsgClass(classOf[SparkConsumersBatchMasterGuardian.Job])

      val expectedException = new Exception("error")

      probe.send(master, SparkConsumersBatchMasterGuardian.JobFailed(jobMessage.model, jobMessage.instance, expectedException))

      probe.expectMsg("OK")


      assert(mockBl.instances().instancesOf("job").exists(_.status==JobStatus.FAILED))

      watch(master)

      master ! SparkConsumersBatchMasterGuardian.Terminate

      expectTerminated(master)


    }

  }


}
