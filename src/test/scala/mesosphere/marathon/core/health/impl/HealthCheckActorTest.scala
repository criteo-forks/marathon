package mesosphere.marathon
package core.health.impl

import akka.NotUsed
import akka.actor.{ActorRef, Props}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{MergeHub, Sink}
import akka.testkit._
import mesosphere.AkkaUnitTest
import mesosphere.marathon.core.health.impl.AppHealthCheckActor.PurgeHealthCheckStatuses
import mesosphere.marathon.core.health.{AntiSnowballApi, Health, HealthCheck, HealthCheckShieldApi, Healthy, MarathonHealthCheck, MarathonHttpHealthCheck, PortReference}
import mesosphere.marathon.core.instance.{Instance, TestInstanceBuilder}
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.termination.{KillReason, KillService}
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.state.{AbsolutePathId, AppDefinition, Timestamp}
import org.mockito.Mockito.verifyNoMoreInteractions

import scala.concurrent.Future
import scala.concurrent.duration._
import mesosphere.marathon.state.UpgradeStrategy

class HealthCheckActorTest extends AkkaUnitTest {
  class Fixture {
    implicit val mat: ActorMaterializer = ActorMaterializer()

    val instanceTracker = mock[InstanceTracker]

    val appId = AbsolutePathId("/test")
    val appVersion = Timestamp(1)
    val app = AppDefinition(id = appId, role = "*", instances = 10, upgradeStrategy = UpgradeStrategy(0.9, 0.1))
    val killService: KillService = mock[KillService]

    val scheduler: MarathonScheduler = mock[MarathonScheduler]

    val instanceBuilder = TestInstanceBuilder.newBuilder(appId, version = appVersion).addTaskRunning()
    val instance = instanceBuilder.getInstance()

    val appHealthCheckActor = TestProbe()
    val healthCheck = MarathonHttpHealthCheck(portIndex = Some(PortReference(0)), interval = 1.second)
    val task: Task = instance.appTask

    val unreachableInstance = TestInstanceBuilder.newBuilder(appId).addTaskUnreachable().getInstance()
    val unscheduledInstance = TestInstanceBuilder.newBuilder(appId).getInstance()
    val lostInstance = TestInstanceBuilder.newBuilder(appId).addTaskLost().getInstance()

    val healthCheckWorkerHub: Sink[(AppDefinition, Instance, MarathonHealthCheck, ActorRef), NotUsed] =
      MergeHub
        .source[(AppDefinition, Instance, MarathonHealthCheck, ActorRef)](1)
        .map { case (_, instance, _, ref) => ref ! Healthy(instance.instanceId, instance.appTask.taskId, Timestamp.now()) }
        .to(Sink.ignore)
        .run()

    val healthCheckShieldApi = mock[HealthCheckShieldApi]
    val antiSnowballApi = mock[AntiSnowballApi]

    def runningInstance(): Instance = {
      TestInstanceBuilder.newBuilder(appId).addTaskRunning().getInstance()
    }

    def actor(healthCheck: HealthCheck, instances: Seq[Instance], app: AppDefinition = app) = TestActorRef[HealthCheckActor](
      Props(
        new HealthCheckActor(app, appHealthCheckActor.ref, killService, healthCheck, instanceTracker, system.eventStream, healthCheckWorkerHub, healthCheckShieldApi, antiSnowballApi) {
          instances.map(instance => {
            val taskId = instance.appTask.taskId
            healthByTaskId += (taskId -> Health(instance.instanceId)
              .update(Healthy(instance.instanceId, taskId, instance.runSpecVersion)))
          })
        }
      )
    )

    def healthCheckActor() = TestActorRef[HealthCheckActor](
      Props(
        new HealthCheckActor(
          app,
          appHealthCheckActor.ref,
          killService,
          healthCheck,
          instanceTracker,
          system.eventStream,
          healthCheckWorkerHub,
          healthCheckShieldApi,
          antiSnowballApi) {
        }
      )
    )
  }

  "HealthCheckActor" should {
    //regression test for #934
    "should not dispatch health checks for staging tasks" in new Fixture {
      instanceTracker.specInstances(any, anyBoolean)(any) returns Future.successful(Seq(instance))

      val actor = healthCheckActor()

      appHealthCheckActor.expectMsgAllClassOf(classOf[PurgeHealthCheckStatuses])
    }

    "should not dispatch health checks for lost tasks" in new Fixture {
      instanceTracker.specInstances(any, anyBoolean)(any) returns Future.successful(Seq(lostInstance))

      val actor = healthCheckActor()

      appHealthCheckActor.expectMsgAllClassOf(classOf[PurgeHealthCheckStatuses])
    }

    "should not dispatch health checks for unreachable tasks" in new Fixture {
      instanceTracker.specInstances(any, anyBoolean)(any) returns Future.successful(Seq(unreachableInstance))

      val actor = healthCheckActor()

      appHealthCheckActor.expectMsgAllClassOf(classOf[PurgeHealthCheckStatuses])
    }

    // regression test for #1456
    "task should be killed if health check fails" in {
      val f = new Fixture
      val healthyInstances = Seq.tabulate(9)(_ => f.runningInstance())
      val unhealthyInstance = f.instance
      val instances = healthyInstances.union(Seq(unhealthyInstance))
      val actor = f.actor(MarathonHttpHealthCheck(maxConsecutiveFailures = 3, portIndex = Some(PortReference(0))), instances)
      f.instanceTracker.specInstancesSync(any, anyBoolean) returns instances

      actor.underlyingActor.checkConsecutiveFailures(unhealthyInstance, Health(unhealthyInstance.instanceId, consecutiveFailures = 3))

      verify(f.killService).killInstancesAndForget(Seq(unhealthyInstance), KillReason.FailedHealthChecks)
      verifyNoMoreInteractions(f.scheduler)
    }

    "task should not be killed if health check fails and not enough tasks are running" in {
      val f = new Fixture
      val instances = Seq.tabulate(8)(_ => f.runningInstance())
      val actor = f.actor(MarathonHttpHealthCheck(maxConsecutiveFailures = 3, portIndex = Some(PortReference(0))), instances)
      f.instanceTracker.specInstancesSync(any, anyBoolean) returns instances

      actor.underlyingActor.checkConsecutiveFailures(f.instance, Health(f.instance.instanceId, consecutiveFailures = 3))

      verify(f.instanceTracker).specInstancesSync(f.appId)
      verifyNoMoreInteractions(f.scheduler, f.killService)
    }

    "unscheduled instance should not make actor crash" in {
      val f = new Fixture
      val healthyInstances = Seq.tabulate(9)(_ => f.runningInstance())
      val instances = healthyInstances.union(Seq(f.unscheduledInstance))
      val actor = f.actor(MarathonHttpHealthCheck(maxConsecutiveFailures = 3, portIndex = Some(PortReference(0))), healthyInstances)

      noException shouldBe thrownBy {
        actor.underlyingActor.purgeStatusOfDoneInstances(instances)
      }
    }

    "task should always be killed if application doesn't set upgradeStrategy.minimumHealthCapacity" in {
      val f = new Fixture
      val healthyInstances = Seq.tabulate(9)(_ => f.runningInstance())
      val unhealthyInstance = f.instance
      val instances = healthyInstances.union(Seq(unhealthyInstance))
      val appWithoutAntiSnowball = AppDefinition(id = f.appId, instances = 10, role = "*")
      val actor = f.actor(MarathonHttpHealthCheck(maxConsecutiveFailures = 3, portIndex = Some(PortReference(0))), instances, appWithoutAntiSnowball)
      f.instanceTracker.specInstancesSync(any, anyBoolean) returns instances

      actor.underlyingActor.checkConsecutiveFailures(unhealthyInstance, Health(unhealthyInstance.instanceId, consecutiveFailures = 3))

      verify(f.killService).killInstancesAndForget(Seq(unhealthyInstance), KillReason.FailedHealthChecks)
      verifyNoMoreInteractions(f.scheduler)
    }

    "task should always be killed if application set only upgradeStrategy.maximumOverCapacity" in {
      val f = new Fixture
      val healthyInstances = Seq.tabulate(9)(_ => f.runningInstance())
      val unhealthyInstance = f.instance
      val instances = healthyInstances.union(Seq(unhealthyInstance))
      val appWithoutAntiSnowball = AppDefinition(id = f.appId, instances = 10, role = "*", upgradeStrategy = UpgradeStrategy(1.0, 0.1))
      val actor = f.actor(MarathonHttpHealthCheck(maxConsecutiveFailures = 3, portIndex = Some(PortReference(0))), instances, appWithoutAntiSnowball)
      f.instanceTracker.specInstancesSync(any, anyBoolean) returns instances

      actor.underlyingActor.checkConsecutiveFailures(unhealthyInstance, Health(unhealthyInstance.instanceId, consecutiveFailures = 3))

      verify(f.killService).killInstancesAndForget(Seq(unhealthyInstance), KillReason.FailedHealthChecks)
      verifyNoMoreInteractions(f.scheduler)
    }

    // FIXME disabling this test for now, as the f.unreachableInstance is broken and does not provide an unreachable instance
    // "task should not be killed if health check fails, but the task is unreachable" in {
    //   val f = new Fixture
    //   val actor = f.actor(MarathonHttpHealthCheck(maxConsecutiveFailures = 3, portIndex = Some(PortReference(0))))

    //   when(f.instanceTracker.countActiveSpecInstances(any)) thenReturn (Future(10))
    //   actor.underlyingActor.checkConsecutiveFailures(f.unreachableInstance, Health(f.unreachableInstance.instanceId, consecutiveFailures = 3))
    //   verify(f.instanceTracker).countActiveSpecInstances(f.appId)
    //   verifyNoMoreInteractions(f.instanceTracker, f.driver, f.scheduler, f.killService)
    // }
  }
}
