package mesosphere.marathon
package core.health.impl

import akka.NotUsed
import akka.actor.{Actor, ActorRef, Props}
import akka.event.EventStream
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.task._
import mesosphere.marathon.core.event._
import mesosphere.marathon.core.health._
import mesosphere.marathon.core.health.impl.AppHealthCheckActor.{ApplicationKey, HealthCheckStatesRequest, HealthCheckStatesResponse, HealthCheckStatusChanged, InstanceKey, PurgeHealthCheckStatuses}
import mesosphere.marathon.core.health.impl.HealthCheckActor._
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.task.termination.{KillReason, KillService}
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.state.{AppDefinition, Timestamp}

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

private[health] class HealthCheckActor(
    app: AppDefinition,
    appHealthCheckActor: ActorRef,
    killService: KillService,
    healthCheck: HealthCheck,
    instanceTracker: InstanceTracker,
    eventBus: EventStream,
    healthCheckHub: Sink[(AppDefinition, Instance, MarathonHealthCheck, ActorRef), NotUsed],
    healthCheckShieldApi: HealthCheckShieldApi)
  extends Actor with StrictLogging {

  implicit val mat = ActorMaterializer()
  import context.dispatcher

  val healthByTaskId = TrieMap.empty[Task.Id, Health]
  var killingInFlight = Set.empty[Task.Id]

  private case class HealthCheckStreamStopped(thisInstance: this.type)

  override def preStart(): Unit = {
    healthCheck match {
      case marathonHealthCheck: MarathonHealthCheck =>
        //Start health checking not after the default first health check
        val startAfter = math.min(marathonHealthCheck.interval.toMillis, HealthCheck.DefaultFirstHealthCheckAfter.toMillis).millis

        logger.info(s"Starting health check for ${app.id} version ${app.version} and healthCheck $marathonHealthCheck in $startAfter ms")

        Source
          .tick(startAfter, marathonHealthCheck.interval, Tick)
          .mapAsync(1)(_ => instanceTracker.specInstances(app.id))
          .map { instances =>
            purgeStatusOfDoneInstances(instances)
            instances.collect {
              case instance if instance.runSpecVersion == app.version && instance.isRunning =>
                logger.debug("Making a health check request for {}", instance.instanceId)
                (app, instance, marathonHealthCheck, self)
            }
          }
          .mapConcat(identity)
          .watchTermination(){ (_, done) =>
            done.onComplete {
              case Success(_) =>
                logger.info(s"HealthCheck stream for app ${app.id} version ${app.version} and healthCheck $healthCheck was stopped")
                self ! HealthCheckStreamStopped(this)

              case Failure(ex) =>
                logger.warn(s"HealthCheck stream for app ${app.id} version ${app.version} and healthCheck $healthCheck crashed due to:", ex)
                self ! HealthCheckStreamStopped(this)
            }
          }
          .runWith(healthCheckHub)
      case _ => // Don't do anything for Mesos health checks
    }
  }

  def getActiveTaskForInstances(instances: Seq[Instance]): Set[Task.Id] = {
    instances.filter(_.tasksMap.size != 0).map(_.appTask).filter(_.isActive).map(_.taskId)(collection.breakOut)
  }

  def purgeStatusOfDoneInstances(instances: Seq[Instance]): Unit = {
    logger.debug(s"Purging health status of inactive instances for app ${app.id} version ${app.version} and healthCheck ${healthCheck}")

    val activeTaskIds: Set[Task.Id] = getActiveTaskForInstances(instances)
    healthByTaskId.retain((taskId, health) => activeTaskIds(taskId))
    // FIXME: I discovered this is unsafe since killingInFlight might be used in 2 concurrent threads (see preStart method above)
    killingInFlight &= activeTaskIds
    logger.info(s"[anti-snowball] app ${app.id} version ${app.version} currently ${killingInFlight.size} instances killingInFlight")

    val checksToPurge = instances.withFilter(!_.isActive).map(instance => {
      val instanceKey = InstanceKey(ApplicationKey(instance.runSpecId, instance.runSpecVersion), instance.instanceId)
      (instanceKey, healthCheck)
    })
    appHealthCheckActor ! PurgeHealthCheckStatuses(checksToPurge)
  }

  def checkConsecutiveFailures(instance: Instance, health: Health): Unit = {
    val consecutiveFailures = health.consecutiveFailures
    val maxFailures = healthCheck.maxConsecutiveFailures

    // ignore failures if maxFailures == 0
    if (consecutiveFailures >= maxFailures && maxFailures > 0) {
      val instanceId = instance.instanceId
      logger.info(
        s"Detected unhealthy $instanceId of app [${app.id}] version [${app.version}] on host ${instance.hostname}"
      )

      // kill the instance, if it is reachable
      if (instance.isUnreachable) {
        logger.info(s"Instance $instanceId on host ${instance.hostname} is temporarily unreachable. Performing no kill.")
      } else {
        require(instance.tasksMap.size == 1, "Unexpected pod instance in HealthCheckActor")
        if (healthCheckShieldApi.isShielded(instance.appTask.taskId)) {
          logger.info(s"[health-check-shield] app ${app.id} version ${app.version}. Won't kill $instanceId because the shield is enabled")
          return
        }

        val activeTaskIds: Set[Task.Id] = getActiveTaskIds()

        if (antiSnowballEnabled && needInfoFromAppHealthCheckActor(activeTaskIds)) {
          logger.debug(s"[anti-snowball] app ${app.id} version ${app.version} Requesting info from AppHealthCheckActor regarding $instanceId")
          appHealthCheckActor ! HealthCheckStatesRequest(instance, health, app.id)
          return
        }

        if (antiSnowballEnabled && !(checkEnoughInstancesRunning(instance, activeTaskIds))) {
          logger.info(s"[anti-snowball] app ${app.id} version ${app.version} Won't kill $instanceId because too few instances are running")
          return
        }

        sendKillRequest(instance, health)
      }
    }
  }

  def sendKillRequest(instance: Instance, health: Health): Unit = {
    val instanceId = instance.instanceId
    logger.info(s"Send kill request for $instanceId on host ${instance.hostname.getOrElse("unknown")} to driver")
    val taskId = instance.appTask.taskId
    eventBus.publish(
      UnhealthyInstanceKillEvent(
        appId = instance.runSpecId,
        taskId = taskId,
        instanceId = instanceId,
        version = app.version,
        reason = health.lastFailureCause.getOrElse("unknown"),
        host = instance.hostname.getOrElse("unknown"),
        slaveId = instance.agentInfo.flatMap(_.agentId),
        timestamp = health.lastFailure.getOrElse(Timestamp.now()).toString
      )
    )
    killingInFlight = killingInFlight + taskId
    logger.info(s"[anti-snowball] app ${app.id} version ${app.version} killing ${instanceId}, currently ${killingInFlight.size} instances killingInFlight")
    killService.killInstancesAndForget(Seq(instance), KillReason.FailedHealthChecks)
  }

  def antiSnowballEnabled(): Boolean = {
    app.upgradeStrategy.minimumHealthCapacity < 1
  }

  def getActiveTaskIds(): Set[Task.Id] = {
    val instances: Seq[Instance] = instanceTracker.specInstancesSync(app.id)
    getActiveTaskForInstances(instances)
  }

  /** Check if HealthCheckActor manages enough instances to decide whether it can kill one or if it needs info from AppHealthCheckActor */
  def needInfoFromAppHealthCheckActor(activeTaskIds: Set[Task.Id]): Boolean = {
    val managedInstances = healthByTaskId.filterKeys(activeTaskIds)
    val enoughInstancesRunning = activeTaskIds.size >= app.instances * app.upgradeStrategy.minimumHealthCapacity
    val enoughManagedInstances = managedInstances.size >= 1 + (activeTaskIds.size * app.upgradeStrategy.minimumHealthCapacity).toInt
    enoughInstancesRunning && !enoughManagedInstances
  }

  /** Check if enough active and ready instances will remain if we kill 1 unhealthy instance */
  def checkEnoughInstancesRunning(unhealthyInstance: Instance, activeTaskIds: Set[Task.Id]): Boolean = {
    val healthyInstances = healthByTaskId.filterKeys(activeTaskIds)
      .filterKeys(taskId => !killingInFlight(taskId))

    logger.info(s"[anti-snowball] app ${app.id} version ${app.version} currently ${killingInFlight.size} instances killingInFlight")

    val futureHealthyInstances = healthyInstances.filterKeys(taskId => unhealthyInstance.appTask.taskId != taskId)
      .count{ case (_, health) => health.ready }

    val futureHealthyCapacity: Double = futureHealthyInstances / app.instances.toDouble
    logger.debug(s"[anti-snowball] app ${app.id} version ${app.version} checkEnoughInstancesRunning: $futureHealthyCapacity >= ${app.upgradeStrategy.minimumHealthCapacity}")
    futureHealthyCapacity >= app.upgradeStrategy.minimumHealthCapacity
  }

  def ignoreFailures(instance: Instance, health: Health): Boolean = {
    // ignore all failures during the grace period aa well as for instances that are not running
    if (instance.isRunning) {
      // ignore if we haven't had a successful health check yet and are within the grace period
      health.firstSuccess.isEmpty && instance.state.since + healthCheck.gracePeriod > Timestamp.now()
    } else {
      true
    }
  }

  def handleHealthResult(result: HealthResult): Unit = {
    val instanceId = result.instanceId
    val health = healthByTaskId.getOrElse(result.taskId, Health(instanceId))

    val updatedHealth = result match {
      case Healthy(_, _, _, _, _) =>
        Future.successful(health.update(result))
      case Unhealthy(_, _, _, _, _, _) =>
        instanceTracker.instance(instanceId).map {
          case Some(instance) =>
            if (ignoreFailures(instance, health)) {
              // Don't update health
              health
            } else {
              logger.debug("{} is {}", instanceId, result)
              if (result.publishEvent) {
                eventBus.publish(FailedHealthCheck(app.id, instanceId, healthCheck))
              }
              self ! InstanceHealthFailure(instance, health)
              // FIXME here we break the behaviour by sending health update before the
              // consecutive failures check is performed, but the original code was sending
              // the health result before the killing even happened, so it is probably harmless
              health.update(result)
            }
          case None =>
            logger.error(s"Couldn't find instance $instanceId")
            health.update(result)
        }
      case _: Ignored =>
        Future.successful(health) // Ignore and keep the old health
    }
    updatedHealth.onComplete {
      case Success(newHealth) => self ! InstanceHealth(result, health, newHealth)
      case Failure(t) => logger.error(s"An error has occurred: ${t.getMessage}", t)
    }
  }

  def updateInstanceHealth(instanceHealth: InstanceHealth): Unit = {
    val result = instanceHealth.result
    val instanceId = result.instanceId
    val health = instanceHealth.health
    val newHealth = instanceHealth.newHealth

    logger.info(s"Received health result for app [${app.id}] version [${app.version}]: [$result]")

    // Purge outdated health status for task associated with the instance.
    // This avoids a bug with Mesos healthchecks where we keep the status for a
    // dead task (in addition to the living one), thus sometimes leading
    // Marathon to report the task / instance / app as unhealthy while
    // everything is running correctly.
    val healthOfInstanceId = healthByTaskId.find(_._1.instanceId == instanceId)
    if (healthOfInstanceId.isDefined)
      healthByTaskId.remove(healthOfInstanceId.get._1)

    healthByTaskId += (result.taskId -> instanceHealth.newHealth)
    appHealthCheckActor ! HealthCheckStatusChanged(ApplicationKey(app.id, app.version), healthCheck, newHealth)

    if (health.alive != newHealth.alive && result.publishEvent) {
      eventBus.publish(HealthStatusChanged(app.id, instanceId, result.version, alive = newHealth.alive))
    }
  }

  def handleHealthCheckStatesResponse(instance: Instance, health: Health, healths: Map[Instance.Id, Option[Boolean]]): Unit = {
    val healthyInstances = healths.count { case(_, health) => health.isDefined && health.get }

    if(healthyInstances / app.instances.toDouble >= app.upgradeStrategy.minimumHealthCapacity) {
      // TODO check killingInFlight?
      sendKillRequest(instance, health)
    } else {
      logger.info(s"[anti-snowball] app ${app.id} version ${app.version} Won't kill ${instance.instanceId} because too few instances are running")
    }
  }

  def receive: Receive = {
    case GetInstanceHealth(instanceId) =>
      sender() ! healthByTaskId.find(_._1.instanceId == instanceId)
        .map(_._2).getOrElse(Health(instanceId))

    case GetAppHealth =>
      sender() ! AppHealth(healthByTaskId.values.to[Seq])

    case result: HealthResult if result.version == app.version =>
      handleHealthResult(result)

    case instanceHealth: InstanceHealth =>
      updateInstanceHealth(instanceHealth)

    case InstanceHealthFailure(instance, health) =>
      checkConsecutiveFailures(instance, health)

    case HealthCheckStreamStopped(thisInstance) =>
      if (thisInstance == this)
        throw new RuntimeException("HealthCheckActor stream stopped, restarting")
      else
        logger.info("Stream pertaining to previous instance of actor stopped; ignoring.")

    case HealthCheckStatesResponse(instance, health, healths) =>
      handleHealthCheckStatesResponse(instance, health, healths)
  }
}

object HealthCheckActor {
  def props(
    app: AppDefinition,
    appHealthCheckActor: ActorRef,
    killService: KillService,
    healthCheck: HealthCheck,
    instanceTracker: InstanceTracker,
    eventBus: EventStream,
    healthCheckHub: Sink[(AppDefinition, Instance, MarathonHealthCheck, ActorRef), NotUsed],
    healthCheckShieldApi: HealthCheckShieldApi): Props = {

    Props(new HealthCheckActor(
      app,
      appHealthCheckActor,
      killService,
      healthCheck,
      instanceTracker,
      eventBus,
      healthCheckHub,
      healthCheckShieldApi))
  }

  // self-sent every healthCheck.intervalSeconds
  case object Tick
  case class GetInstanceHealth(instanceId: Instance.Id)
  case object GetAppHealth

  case class AppHealth(health: Seq[Health])

  case class InstanceHealth(result: HealthResult, health: Health, newHealth: Health)
  case class InstancesUpdate(version: Timestamp, instances: Seq[Instance])

  case class InstanceHealthFailure(instance: Instance, health: Health)
}
