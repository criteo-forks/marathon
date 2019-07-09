package mesosphere.marathon
package core.health.impl

import akka.Done
import akka.actor.{Actor, ActorRef, Cancellable, Props}
import akka.event.EventStream
import akka.stream.Materializer
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.event._
import mesosphere.marathon.core.health._
import mesosphere.marathon.core.health.impl.AppHealthCheckActor.{ApplicationKey, HealthCheckStatusChanged, InstanceKey, PurgeHealthCheckStatuses}
import mesosphere.marathon.core.health.impl.HealthCheckActor._
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.task.termination.{KillReason, KillService}
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.state.{AppDefinition, Timestamp}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Try, Failure, Success}
import scala.language.postfixOps

private[health] class HealthCheckActor(
    app: AppDefinition,
    appHealthCheckActor: ActorRef,
    killService: KillService,
    healthCheck: HealthCheck,
    instanceTracker: InstanceTracker,
    eventBus: EventStream)(implicit mat: Materializer) extends Actor with StrictLogging {

  import HealthCheckWorker.HealthCheckJob
  import context.dispatcher

  var nextScheduledCheck: Option[Cancellable] = None
  var healthByInstanceId = Map.empty[Instance.Id, Health]

  val workerProps: Props = Props(classOf[HealthCheckWorkerActor], mat)

  override def preStart(): Unit = {
    logger.info(
      "Starting health check actor for app [{}] version [{}] and healthCheck [{}]",
      app.id,
      app.version,
      healthCheck
    )
    //Start health checking not after the default first health check
    val start = math.min(healthCheck.interval.toMillis, HealthCheck.DefaultFirstHealthCheckAfter.toMillis).millis
    scheduleNextHealthCheck(Some(start))
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit =
    logger.info(
      "Restarting health check actor for app [{}] version [{}] and healthCheck [{}]",
      app.id,
      app.version,
      healthCheck
    )

  override def postStop(): Unit = {
    nextScheduledCheck.forall { _.cancel() }
    logger.info(
      "Stopped health check actor for app [{}] version [{}] and healthCheck [{}]",
      app.id,
      app.version,
      healthCheck
    )
  }

  def updateInstances(): Future[Done] = {
    instanceTracker.specInstances(app.id).map { instances =>
      self ! InstancesUpdate(version = app.version, instances = instances)
      Done
    }.recover {
      case t =>
        logger.error(s"An error has occurred: ${t.getMessage}", t)
        Done
    }
  }

  def purgeStatusOfDoneInstances(instances: Seq[Instance]): Unit = {
    logger.debug(
      "Purging health status of inactive instances for app [{}] version [{}] and healthCheck [{}]",
      app.id,
      app.version,
      healthCheck
    )
    val activeInstanceIds: Set[Instance.Id] = instances.withFilter(_.isActive).map(_.instanceId)(collection.breakOut)
    // The Map built with filterKeys wraps the original map and contains a reference to activeInstanceIds.
    // Therefore we materialize it into a new map.
    healthByInstanceId = healthByInstanceId.filterKeys(activeInstanceIds).iterator.toMap

    val hcToPurge = instances.withFilter(!_.isActive).map(instance => {
      val instanceKey = InstanceKey(ApplicationKey(instance.runSpecId, instance.runSpecVersion), instance.instanceId)
      (instanceKey, healthCheck)
    })
    appHealthCheckActor ! PurgeHealthCheckStatuses(hcToPurge)
  }

  def scheduleNextHealthCheck(interval: Option[FiniteDuration] = None): Unit = healthCheck match {
    case hc: MarathonHealthCheck =>
      logger.debug(
        "Scheduling next health check for app [{}] version [{}] and healthCheck [{}]",
        app.id,
        app.version,
        hc
      )
      nextScheduledCheck = Some(
        context.system.scheduler.scheduleOnce(interval.getOrElse(hc.interval)) {
          self ! Tick
        }
      )
    case _ => // Don't do anything for Mesos health checks
  }

  def dispatchJobs(instances: Seq[Instance]): Unit = healthCheck match {
    case hc: MarathonHealthCheck =>
      logger.debug("Dispatching health check jobs to workers")
      instances.foreach { instance =>
        if (instance.runSpecVersion == app.version && instance.isRunning) {
          logger.debug("Dispatching health check job for {}", instance.instanceId)
          val worker: ActorRef = context.actorOf(workerProps)
          worker ! HealthCheckJob(app, instance, hc)
        }
      }
    case _ => // Don't do anything for Mesos health checks
  }

  def checkConsecutiveFailures(instance: Instance, health: Health): Unit = {
    val consecutiveFailures = health.consecutiveFailures
    val maxFailures = healthCheck.maxConsecutiveFailures

    // ignore failures if maxFailures == 0
    if (consecutiveFailures >= maxFailures && maxFailures > 0) {
      val instanceId = instance.instanceId
      logger.info(
        s"Detected unhealthy $instanceId of app [${app.id}] version [${app.version}] on host ${instance.agentInfo.host}"
      )

      // kill the instance, if it is reachable
      if (instance.isUnreachable) {
        logger.info(s"Instance $instanceId on host ${instance.agentInfo.host} is temporarily unreachable. Performing no kill.")
      } else {
        if (!(checkEnoughInstancesRunning())) {
          logger.info(s"[anti-snowball] Won't kill $instanceId because too few instances are running")
          return
        }
        logger.info(s"Send kill request for $instanceId on host ${instance.agentInfo.host} to driver")
        require(instance.tasksMap.size == 1, "Unexpected pod instance in HealthCheckActor")
        val taskId = instance.appTask.taskId
        eventBus.publish(
          UnhealthyInstanceKillEvent(
            appId = instance.runSpecId,
            taskId = taskId,
            instanceId = instanceId,
            version = app.version,
            reason = health.lastFailureCause.getOrElse("unknown"),
            host = instance.agentInfo.host,
            slaveId = instance.agentInfo.agentId,
            timestamp = health.lastFailure.getOrElse(Timestamp.now()).toString
          )
        )
        val killed = killService.killInstances(Seq(instance), KillReason.FailedHealthChecks)
        Try(Await.ready(killed, 5 seconds)) match {
          case Success(future) => future.value.get match {
            case Success(done) =>
              instanceTracker.countActiveSpecInstances(app.id).onComplete {
                case Success(activeInstances) => logger.info(s"Done killing $instanceId, $activeInstances remaining")
                case Failure(throwable) => logger.warn(s"[anti-snowball] Error when counting instance remaining: ${throwable.getMessage}. Since it is just for logging purpose, we don't care that much")
              }
            case Failure(t) => logger.error(s"An error has occurred: ${t.getMessage}", t)
          }
          case Failure(_) => logger.error(s"Timed out killing instance $instanceId")
        }
      }
    }
  }

  def checkEnoughInstancesRunning(): Boolean = {
    Try(Await.result(instanceTracker.countActiveSpecInstances(app.id), 5 seconds)) match {
      case Success(activeInstances) =>
            val futureHealthyCapacity: Double = (activeInstances - 1) / app.instances.toDouble
            logger.debug(s"[anti-snowball] checkEnoughInstancesRunning: $futureHealthyCapacity >= ${app.upgradeStrategy.minimumHealthCapacity}")
            futureHealthyCapacity >= app.upgradeStrategy.minimumHealthCapacity
      case Failure(_) =>
        logger.error(s"[anti-snowball] Timeout when checking active instances. We'll stay safe")
        false
    }
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
    val health = healthByInstanceId.getOrElse(instanceId, Health(instanceId))

    val updatedHealth = result match {
      case Healthy(_, _, _, _) =>
        Future(health.update(result))
      case Unhealthy(_, _, _, _, _) =>
        instanceTracker.instance(instanceId).map({
          case Some(instance) =>
            if (ignoreFailures(instance, health)) {
              // Don't update health
              health
            } else {
              logger.debug("{} is {}", instance.instanceId, result)
              if (result.publishEvent) {
                eventBus.publish(FailedHealthCheck(app.id, instanceId, healthCheck))
              }
              checkConsecutiveFailures(instance, health)
              health.update(result)
            }
          case None =>
            logger.error(s"Couldn't find instance $instanceId")
            health.update(result)
        })
    }
    Try(Await.ready(updatedHealth, 5 seconds)) match {
      case Success(future) => future.value.get match {
        case Success(newHealth) => self ! InstanceHealth(result, health, newHealth)
        case Failure(t) => logger.error(s"An error has occurred: ${t.getMessage}", t)
      }
      case Failure(_) => logger.error(s"Timed out updating health $instanceId")
    }
  }

  def updateInstanceHealth(instanceHealth: InstanceHealth): Unit = {
    val result = instanceHealth.result
    val instanceId = result.instanceId
    val health = instanceHealth.health
    val newHealth = instanceHealth.newHealth

    logger.info("Received health result for app [{}] version [{}]: [{}]", app.id, app.version, result)
    healthByInstanceId += (instanceId -> instanceHealth.newHealth)
    appHealthCheckActor ! HealthCheckStatusChanged(ApplicationKey(app.id, app.version), healthCheck, newHealth)

    if (health.alive != newHealth.alive && result.publishEvent) {
      eventBus.publish(HealthStatusChanged(app.id, instanceId, result.version, alive = newHealth.alive))
    }
  }

  def receive: Receive = {
    case GetInstanceHealth(instanceId) => sender() ! healthByInstanceId.getOrElse(instanceId, Health(instanceId))

    case GetAppHealth =>
      sender() ! AppHealth(healthByInstanceId.values.to[Seq])

    case Tick =>
      updateInstances().andThen{ case _ => self ! ScheduleNextHealthCheck() }

    case ScheduleNextHealthCheck(interval) =>
      scheduleNextHealthCheck(interval)

    case InstancesUpdate(version, instances) if version == app.version =>
      purgeStatusOfDoneInstances(instances)
      dispatchJobs(instances)

    case result: HealthResult if result.version == app.version =>
      handleHealthResult(result)

    case instanceHealth: InstanceHealth =>
      updateInstanceHealth(instanceHealth)

    case result: HealthResult =>
      logger.warn(s"Ignoring health result [$result] due to version mismatch.")

  }
}

object HealthCheckActor {
  def props(
    app: AppDefinition,
    appHealthCheckActor: ActorRef,
    killService: KillService,
    healthCheck: HealthCheck,
    instanceTracker: InstanceTracker,
    eventBus: EventStream)(implicit mat: Materializer): Props = {

    Props(new HealthCheckActor(
      app,
      appHealthCheckActor,
      killService,
      healthCheck,
      instanceTracker,
      eventBus))
  }

  // self-sent every healthCheck.intervalSeconds
  case object Tick
  case class GetInstanceHealth(instanceId: Instance.Id)
  case object GetAppHealth
  case class ScheduleNextHealthCheck(interval: Option[FiniteDuration] = None)

  case class AppHealth(health: Seq[Health])

  case class InstanceHealth(result: HealthResult, health: Health, newHealth: Health)
  case class InstancesUpdate(version: Timestamp, instances: Seq[Instance])

}
