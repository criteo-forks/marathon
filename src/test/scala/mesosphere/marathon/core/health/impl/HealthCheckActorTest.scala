package mesosphere.marathon
package core.health.impl

import akka.actor.Props
import akka.testkit._
import mesosphere.AkkaUnitTest
import mesosphere.marathon.core.health._
import mesosphere.marathon.core.instance.{Instance, TestInstanceBuilder}
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.termination.{KillReason, KillService}
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.state.PathId._
import mesosphere.marathon.state.{AppDefinition, Timestamp}
import mesosphere.marathon.storage.repository.AppRepository
import org.apache.mesos.SchedulerDriver
import org.mockito.Mockito.{verifyNoMoreInteractions, when}

import scala.concurrent.Future
import mesosphere.marathon.state.UpgradeStrategy

class HealthCheckActorTest extends AkkaUnitTest {
  class Fixture {
    val tracker = mock[InstanceTracker]

    val appId = "/test".toPath
    val appVersion = Timestamp(1)
    val app = AppDefinition(id = appId, instances = 10, upgradeStrategy = UpgradeStrategy(0.9, 1.1))
    val appWithoutAntiSnowball = AppDefinition(id = appId, instances = 10)
    val appRepository: AppRepository = mock[AppRepository]
    val holder: MarathonSchedulerDriverHolder = new MarathonSchedulerDriverHolder
    val driver = mock[SchedulerDriver]
    holder.driver = Some(driver)
    when(appRepository.getVersion(appId, appVersion.toOffsetDateTime)).thenReturn(Future.successful(Some(app)))
    val killService: KillService = mock[KillService]
    when(appRepository.getVersion(appId, appVersion.toOffsetDateTime)).thenReturn(Future.successful(Some(app)))

    val scheduler: MarathonScheduler = mock[MarathonScheduler]

    val instanceBuilder = TestInstanceBuilder.newBuilder(appId, version = appVersion).addTaskRunning()
    val instance = instanceBuilder.getInstance()
    val appHealthCheckActor = TestProbe()

    val task: Task = instance.appTask

    val unreachableInstance = TestInstanceBuilder.newBuilder(appId).addTaskUnreachable().getInstance()
    val unreachableTask: Task = unreachableInstance.appTask

    def runningInstance(): Instance = {
      TestInstanceBuilder.newBuilder(appId).addTaskRunning().getInstance()
    }

    def actor(healthCheck: HealthCheck, instances: Seq[Instance], app: AppDefinition = app) = TestActorRef[HealthCheckActor](
      Props(
        new HealthCheckActor(app, appHealthCheckActor.ref, killService, healthCheck, tracker, system.eventStream) {
          instances.map(instance => {
            healthByInstanceId += (instance.instanceId -> Health(instance.instanceId)
              .update(Healthy(instance.instanceId, instance.version)))
          })
        }
      )
    )

    def actorWithLatch(latch: TestLatch) = TestActorRef[HealthCheckActor](
      Props(
        new HealthCheckActor(
          app,
          appHealthCheckActor.ref,
          killService,
          MarathonHttpHealthCheck(portIndex = Some(PortReference(0))),
          tracker,
          system.eventStream) {

          override val workerProps = Props {
            latch.countDown()
            new TestActors.EchoActor
          }
        }
      )
    )
  }

  "HealthCheckActor" should {
    // regression test for #934
    "should not dispatch health checks for staging tasks" in {
      val f = new Fixture
      val latch = TestLatch(1)
      val appId = "/test".toPath
      val appVersion = Timestamp(1)
      val app = AppDefinition(id = appId)
      val appRepository: AppRepository = mock[AppRepository]

      when(appRepository.getVersion(appId, appVersion.toOffsetDateTime)).thenReturn(Future.successful(Some(app)))

      val actor = f.actorWithLatch(latch)
      actor.underlyingActor.dispatchJobs(Seq(f.instance))
      latch.isOpen should be (false)
      verifyNoMoreInteractions(f.driver)
    }

    "should not dispatch health checks for lost tasks" in {
      val f = new Fixture
      val latch = TestLatch(1)

      val actor = f.actorWithLatch(latch)

      actor.underlyingActor.dispatchJobs(Seq(f.unreachableInstance))
      latch.isOpen should be (false)
      verifyNoMoreInteractions(f.driver)
    }

    "should not dispatch health checks for unreachable tasks" in {
      val f = new Fixture
      val latch = TestLatch(1)

      val actor = f.actorWithLatch(latch)

      actor.underlyingActor.dispatchJobs(Seq(f.unreachableInstance))
      latch.isOpen should be (false)
      verifyNoMoreInteractions(f.driver)
    }

    // regression test for #1456
    "task should be killed if health check fails" in {
      val f = new Fixture
      val healthyInstances = Seq.tabulate(9)(_ => f.runningInstance())
      val unhealthyInstance = f.instance
      val instances = healthyInstances.union(Seq(unhealthyInstance))
      val actor = f.actor(MarathonHttpHealthCheck(maxConsecutiveFailures = 3, portIndex = Some(PortReference(0))), instances)
      f.tracker.specInstancesSync(any) returns instances

      actor.underlyingActor.checkConsecutiveFailures(unhealthyInstance, Health(unhealthyInstance.instanceId, consecutiveFailures = 3))

      verify(f.killService).killInstancesAndForget(Seq(unhealthyInstance), KillReason.FailedHealthChecks)
      verifyNoMoreInteractions(f.driver, f.scheduler)
    }

    "task should not be killed if health check fails and not enough tasks are running" in {
      val f = new Fixture
      val instances = Seq.tabulate(8)(_ => f.runningInstance())
      val actor = f.actor(MarathonHttpHealthCheck(maxConsecutiveFailures = 3, portIndex = Some(PortReference(0))), instances)
      f.tracker.specInstancesSync(any) returns instances
      actor.underlyingActor.checkConsecutiveFailures(f.instance, Health(f.instance.instanceId, consecutiveFailures = 3))
      verify(f.tracker).specInstancesSync(f.appId)
      verifyNoMoreInteractions(f.tracker, f.driver, f.scheduler, f.killService)
    }

    "task should always be killed if application doesn't set upgradeStrategy.minimumHealthCapacity" in {
      val f = new Fixture
      val healthyInstances = Seq.tabulate(9)(_ => f.runningInstance())
      val unhealthyInstance = f.instance
      val instances = healthyInstances.union(Seq(unhealthyInstance))
      val actor = f.actor(MarathonHttpHealthCheck(maxConsecutiveFailures = 3, portIndex = Some(PortReference(0))), instances, f.appWithoutAntiSnowball)
      f.tracker.specInstancesSync(any) returns instances

      actor.underlyingActor.checkConsecutiveFailures(unhealthyInstance, Health(unhealthyInstance.instanceId, consecutiveFailures = 3))

      verify(f.killService).killInstancesAndForget(Seq(unhealthyInstance), KillReason.FailedHealthChecks)
      verifyNoMoreInteractions(f.driver, f.scheduler)
    }

    // FIXME disabling this test for now, as the f.unreachableInstance is broken and does not provide an unreachable instance
    // "task should not be killed if health check fails, but the task is unreachable" in {
    //   val f = new Fixture
    //   val actor = f.actor(MarathonHttpHealthCheck(maxConsecutiveFailures = 3, portIndex = Some(PortReference(0))))

    //   when(f.tracker.countActiveSpecInstances(any)) thenReturn (Future(10))
    //   actor.underlyingActor.checkConsecutiveFailures(f.unreachableInstance, Health(f.unreachableInstance.instanceId, consecutiveFailures = 3))
    //   verify(f.tracker).countActiveSpecInstances(f.appId)
    //   verifyNoMoreInteractions(f.tracker, f.driver, f.scheduler, f.killService)
    // }
  }
}
