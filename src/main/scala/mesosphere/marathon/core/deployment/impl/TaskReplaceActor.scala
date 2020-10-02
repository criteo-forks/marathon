package mesosphere.marathon
package core.deployment.impl

import akka.Done
import akka.actor._
import akka.event.EventStream
import akka.pattern._
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.event._
import mesosphere.marathon.core.health.Health
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.{Goal, GoalChangeReason, Instance}
import mesosphere.marathon.core.launchqueue.LaunchQueue
import mesosphere.marathon.core.readiness.ReadinessCheckExecutor
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.state.RunSpec

import scala.async.Async.{async, await}
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

class TaskReplaceActor(
    val deploymentManagerActor: ActorRef,
    val status: DeploymentStatus,
    val launchQueue: LaunchQueue,
    val instanceTracker: InstanceTracker,
    val eventBus: EventStream,
    val readinessCheckExecutor: ReadinessCheckExecutor,
    val runSpec: RunSpec,
    promise: Promise[Unit]) extends Actor with StrictLogging {
  import TaskReplaceActor._

  def deploymentId = status.plan.id
  def pathId = runSpec.id

  private[this] var tick: Cancellable = null

  @SuppressWarnings(Array("all")) // async/await
  override def preStart(): Unit = {
    super.preStart()
    // subscribe to all needed events
    eventBus.subscribe(self, classOf[HealthStatusResponse])

    // reset the launch queue delay
    logger.info("Resetting the backoff delay before restarting the runSpec")
    launchQueue.resetDelay(runSpec)

    // Fetch state of apps
    val system = akka.actor.ActorSystem("system")
    // FIXME(t.lange): Make those 10 seconds configurable
    tick = system.scheduler.schedule(0 seconds, 10 seconds)(request_health_status)
  }

  def isHealthy(instance: Instance, healths: Map[Instance.Id, Seq[Health]]): Boolean = {
    val i_health = healths.find(_._1 == instance.instanceId)
    if (instance.hasConfiguredHealthChecks && i_health.isDefined)
      return _isHealthy(i_health.get._2)
    else if (instance.hasConfiguredHealthChecks && !i_health.isDefined)
      return false
    else
      return instance.isRunning
  }

  def _isHealthy(healths: Seq[Health]): Boolean = {
    for (h <- healths)
      if (!h.alive) return false
    return true
  }

  def request_health_status(): Unit = {
    logger.info(s"Requesting HealthStatus for ${pathId}")
    deploymentManagerActor ! HealthStatusRequest(pathId)
  }

  // sort instances with instances having oldest tasks first. Instances with no task are at the end.
  def sortByOldestFirst(instances: mutable.Queue[Instance]): mutable.Queue[Instance] = {
    val instancesByIncarnation = instances.partition(_.tasksMap.size != 0)
    instancesByIncarnation._1.sortBy(_.appTask.status.stagedAt) ++ instancesByIncarnation._2
  }

  def step(health: Map[Instance.Id, Seq[Health]]): Unit = {
    logger.debug(s"---=== DEPLOYMENT STEP FOR ${pathId} ===---")
    val current_instances = instanceTracker.specInstancesSync(pathId).partition(_.runSpecVersion == runSpec.version)

    val new_instances = current_instances._1.partition(isHealthy(_, health))
    val old_instances = current_instances._2.partition(isHealthy(_, health))

    // make sure we kill in order:
    // - instances with unhealthy tasks
    // - instances with oldest tasks
    // - other instances
    val toKill = old_instances._2.to[mutable.Queue]
    toKill ++= sortByOldestFirst(old_instances._1.to[mutable.Queue])

    val state = new TransitionState(new_instances._1.size, new_instances._2.size, old_instances._1.size, old_instances._2.size)

    logger.info(s"Found health status for ${pathId}: new_running=>${state.newInstancesRunning} old_running=>${state.oldInstancesRunning} new_failing=>${state.newInstancesFailing} old_failing=>${state.oldInstancesFailing}")

    val restartStrategy = computeRestartStrategy(runSpec, state)

    logger.info(s"restartStrategy for ${pathId} gives : to_kill=>${restartStrategy.nrToKillImmediately} to_start=>${restartStrategy.nrToStartImmediately}")

    // kill old instances to free some capacity
    for (_ <- 0 until restartStrategy.nrToKillImmediately) killNextOldInstance(toKill)

    // start new instances, if possible
    launchInstances(restartStrategy.nrToStartImmediately).pipeTo(self)

    // Check if we reached the end of the deployment, meaning
    // that old instances (failing or running) are removed,
    // and new instances are all running.
    checkFinished(state.newInstancesRunning, state.oldInstances)
  }

  override def postStop(): Unit = {
    tick.cancel()
    eventBus.unsubscribe(self)
    super.postStop()
  }

  override def receive: Receive = {

    case HealthStatusResponse(health) =>
      step(health)

    case Status.Failure(e) =>
      // This is the result of failed launchQueue.addAsync(...) call. Log the message and
      // restart this actor. Next reincarnation should try to start from the beginning.
      logger.warn(s"Deployment $deploymentId: Failed to launch instances: ", e)
      throw e

    case Done => // This is the result of successful launchQueue.addAsync(...) call. Nothing to do here

    case CheckFinished => // Notging to do, this is done in step()

  }

  def launchInstances(instancesToStartNow: Int): Future[Done] = {
    if (instancesToStartNow > 0) {
      logger.info(s"Deployment $deploymentId: Restarting app $pathId: queuing $instancesToStartNow new instances.")
      launchQueue.add(runSpec, instancesToStartNow)
    } else {
      Future.successful(Done)
    }
  }

  @SuppressWarnings(Array("all")) // async/await
  def killNextOldInstance(toKill: mutable.Queue[Instance]): Unit = {
    if (toKill.nonEmpty) {
      val dequeued = toKill.dequeue().instanceId
      async {
        await(instanceTracker.get(dequeued)) match {
          case None =>
            logger.warn(s"Deployment $deploymentId: Was about to kill instance $dequeued but it did not exist in the instance tracker anymore.")
          case Some(nextOldInstance) =>
            logger.info(s"Deployment $deploymentId: Killing old ${nextOldInstance.instanceId}")
            val goal = if (runSpec.isResident) Goal.Stopped else Goal.Decommissioned
            await(instanceTracker.setGoal(nextOldInstance.instanceId, goal, GoalChangeReason.Upgrading))
        }
      }
    }
  }

  def checkFinished(newInstances: Int, oldInstances: Int): Unit = {
    if (newInstances == runSpec.instances && oldInstances == 0) {
      logger.info(s"Deployment $deploymentId: All new instances for $pathId are ready and all old instances have been killed")
      promise.trySuccess(())
      context.stop(self)
    } else {
      logger.info(s"Deployment $deploymentId: For run spec: [${runSpec.id}] there are " +
        s"[${newInstances}] ready new instances and " +
        s"[${oldInstances}] old instances. " +
        s"Target count is ${runSpec.instances}.")
    }
  }
}

object TaskReplaceActor extends StrictLogging {

  object CheckFinished

  //scalastyle:off
  def props(
    deploymentManagerActor: ActorRef,
    status: DeploymentStatus,
    launchQueue: LaunchQueue,
    instanceTracker: InstanceTracker,
    eventBus: EventStream,
    readinessCheckExecutor: ReadinessCheckExecutor,
    app: RunSpec,
    promise: Promise[Unit]): Props = Props(
    new TaskReplaceActor(deploymentManagerActor, status, launchQueue, instanceTracker, eventBus,
      readinessCheckExecutor, app, promise)
  )

  /** Encapsulates the logic how to get a Restart going */
  private[impl] case class RestartStrategy(nrToKillImmediately: Int, nrToStartImmediately: Int, maxCapacity: Int)

  /** Encapsulates the logic of how the transition from old runspec to new is going */
  class TransitionState(var newInstancesRunning: Int = 0, var newInstancesFailing: Int = 0, var oldInstancesRunning: Int = 0, var oldInstancesFailing: Int = 0) {
    def oldInstances = oldInstancesFailing + oldInstancesRunning
    def newInstances = newInstancesFailing + newInstancesRunning
    def instancesCount = oldInstances + newInstances
  }

  private[impl] def computeRestartStrategy(runSpec: RunSpec, state: TransitionState): RestartStrategy = {
    val consideredHealthyInstancesCount = state.newInstancesRunning + state.oldInstancesRunning
    val consideredUnhealthyInstancesCount = state.newInstancesFailing + state.oldInstancesFailing
    val totalInstancesRunning = state.instancesCount

    // in addition to a spec which passed validation, we require:
    require(runSpec.instances > 0, s"target instance number must be > 0 but is ${runSpec.instances}")
    require(totalInstancesRunning >= 0, "current instances count must be >=0")

    // Old and new instances that are running & are considered healthy
    require(consideredHealthyInstancesCount >= 0, s"running instances count must be >=0 but is $consideredHealthyInstancesCount")

    val minHealthy = (runSpec.instances * runSpec.upgradeStrategy.minimumHealthCapacity).ceil.toInt
    var maxCapacity = (runSpec.instances * (1 + runSpec.upgradeStrategy.maximumOverCapacity)).toInt
    var nrToKillImmediately = math.min(math.max(0, consideredHealthyInstancesCount - minHealthy), state.oldInstances)

    if (minHealthy == maxCapacity && maxCapacity <= consideredHealthyInstancesCount) {
      if (runSpec.isResident) {
        // Kill enough instances so that we end up with one instance below minHealthy.
        // TODO: We need to do this also while restarting, since the kill could get lost.
        nrToKillImmediately = consideredHealthyInstancesCount - minHealthy + 1
        logger.info(
          "maxCapacity == minHealthy for resident app: " +
            s"adjusting nrToKillImmediately to $nrToKillImmediately in order to prevent over-capacity for resident app"
        )
      } else {
        logger.info("maxCapacity == minHealthy: Allow temporary over-capacity of one instance to allow restarting")
        maxCapacity += 1
      }
    }

    // following condition addresses cases where we have extra-instances due to previous deployment adding extra-instances
    // and deployment is force-updated
    if (runSpec.instances < state.newInstancesRunning + state.oldInstances) {
      // NOTE: We don't take into account the new app that are failing to count this
      // This is to avoid killing apps started but not ready
      nrToKillImmediately = math.max(state.newInstancesRunning + state.oldInstances - runSpec.instances, nrToKillImmediately)
      logger.info(s"runSpec.instances < currentInstances: Allowing killing all $nrToKillImmediately extra-instances")
    }

    logger.info(s"For minimumHealthCapacity ${runSpec.upgradeStrategy.minimumHealthCapacity} of ${runSpec.id.toString} leave " +
      s"$minHealthy instances running, maximum capacity $maxCapacity, killing $nrToKillImmediately of " +
      s"$consideredHealthyInstancesCount running instances immediately. (RunSpec version ${runSpec.version})")

    assume(nrToKillImmediately >= 0, s"nrToKillImmediately must be >=0 but is $nrToKillImmediately")
    assume(maxCapacity > 0, s"maxCapacity must be >0 but is $maxCapacity")

    val leftCapacity = math.max(0, maxCapacity - totalInstancesRunning)
    val instancesNotStartedYet = math.max(0, runSpec.instances - state.newInstances)
    val nrToStartImmediately = math.min(instancesNotStartedYet, leftCapacity)
    logger.info(s"For maxCapacity ${maxCapacity}, leftCapacity ${leftCapacity} and still not started ${instancesNotStartedYet}, will start ${nrToStartImmediately} now!")
    RestartStrategy(nrToKillImmediately = nrToKillImmediately, nrToStartImmediately = nrToStartImmediately, maxCapacity = maxCapacity)
  }
}

