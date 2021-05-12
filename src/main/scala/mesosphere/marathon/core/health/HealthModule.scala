package mesosphere.marathon
package core.health

import akka.actor.ActorSystem
import akka.event.EventStream
import akka.stream.ActorMaterializer
import mesosphere.marathon.core.group.GroupManager
import mesosphere.marathon.core.health.impl.MarathonHealthCheckManager
import mesosphere.marathon.core.task.termination.KillService
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.storage.repository.HealthCheckShieldRepository
import mesosphere.marathon.core.health.impl.{HealthCheckShieldApiImpl, HealthCheckShieldActor, AntiSnowballApiImpl}
import mesosphere.marathon.core.leadership.LeadershipModule

import scala.concurrent.ExecutionContext

/**
  * Exposes everything related to a task health, including the health check manager.
  */
class HealthModule(
    actorSystem: ActorSystem,
    killService: KillService,
    eventBus: EventStream,
    instanceTracker: InstanceTracker,
    groupManager: GroupManager,
    conf: MarathonConf,
    healthCheckShieldRepository: HealthCheckShieldRepository,
    leadershipModule: LeadershipModule)(implicit mat: ActorMaterializer, ec: ExecutionContext) {
  private val healthCheckShieldApiImpl = new HealthCheckShieldApiImpl(healthCheckShieldRepository, conf)
  private val antiSnowballApiImpl = new AntiSnowballApiImpl()
  private val healthCheckShieldActor = leadershipModule.startWhenLeader(
    HealthCheckShieldActor.props(healthCheckShieldApiImpl, conf),
    "HealthCheckShieldActor")

  lazy val healthCheckShieldApi: HealthCheckShieldApi = healthCheckShieldApiImpl
  lazy val antiSnowballApi: AntiSnowballApi = antiSnowballApiImpl
  lazy val healthCheckManager = new MarathonHealthCheckManager(
    actorSystem,
    killService,
    eventBus,
    instanceTracker,
    groupManager,
    conf,
    healthCheckShieldApi,
    antiSnowballApi)
}
