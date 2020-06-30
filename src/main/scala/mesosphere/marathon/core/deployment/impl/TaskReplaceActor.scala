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

  // instance to kill sorted by decreasing order of priority
  // we always prefer to kill unhealthy tasks first
  private[this] val toKillOrdered = instanceTracker.specInstancesSync(runSpec.id, readAfterWrite = true)
    .partition(_.runSpecVersion == runSpec.version)._2.sortWith((i1, i2) => {
      (i1.consideredHealthy, i2.consideredHealthy) match {
        case (_, false) => false
        case _ => true
      }
    })

  // All instances to kill queued up
  private[this] val toKill: mutable.Queue[Instance.Id] = toKillOrdered.map(_.instanceId).to[mutable.Queue]

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
    tick = system.scheduler.schedule(0 seconds, 10 seconds)(request_health_status)
  }

  def isHealthy(instance: Instance, healths: Map[Instance.Id, Seq[Health]]): Boolean = {
    // FIXME: Or use state.healthy.getOrElse
    val i_health = healths.find(_._1 == instance.instanceId)
    if (instance.hasConfiguredHealthChecks && i_health.isDefined) { // Has healthchecks
      if (_isHealthy(i_health.get._2) == true) {
        logger.info(s"Checking ${instance.instanceId}: has healcheck and is healthy")
        return true
      } else {
        logger.info(s"Checking ${instance.instanceId}: has healcheck and is NOT healthy")
        return false
      }
    } else if (instance.hasConfiguredHealthChecks && !i_health.isDefined) {
      logger.info(s"Checking ${instance.instanceId}: has healcheck and is UNKNOWN")
      return false
    } else { // Don't have healthchecks
      logger.info(s"Checking ${instance.instanceId}: has now HC")
      if (instance.state.goal == Goal.Running) return true
      else return false
    }
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

  def step(health: Map[Instance.Id, Seq[Health]]): Unit = {
    logger.info("---=== DEPLOYMENT STEP FOR ${pathId} ===---yy")
    val current_instances = instanceTracker.specInstancesSync(pathId)
    var new_running = 0
    var old_running = 0
    var new_failing = 0
    var old_failing = 0
    for (i <- current_instances) {
      if (i.runSpecVersion == runSpec.version) { // New version
        if (isHealthy(i, health)) new_running += 1
        else new_failing += 1
      } else { // Old instance
        if (isHealthy(i, health)) old_running += 1
        else old_failing += 1
      }
    }

    logger.info(s"=> New running: ${new_running}")
    logger.info(s"=> New failing: ${new_failing}")
    logger.info(s"=> Old running: ${old_running}")
    logger.info(s"=> Old failing: ${old_failing}")

    val ignitionStrategy = computeRestartStrategy(runSpec, new_running, old_running, new_failing, old_failing)

    logger.info(s"=> To kill: ${ignitionStrategy.nrToKillImmediately}")
    logger.info(s"=> To start: ${ignitionStrategy.nrToStartImmediately}")

    // kill old instances to free some capacity
    for (_ <- 0 until ignitionStrategy.nrToKillImmediately) killNextOldInstance()

    // start new instances, if possible
    launchInstances(ignitionStrategy.nrToStartImmediately).pipeTo(self)

    // Check if we reached the end of the deployment
    checkFinished(new_running, old_running + old_failing)
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
      logger.info(s"Deployment $deploymentId: Restarting app $pathId. No need to start new instances right now.")
      Future.successful(Done)
    }
  }

  @SuppressWarnings(Array("all")) // async/await
  def killNextOldInstance(maybeNewInstanceId: Option[Instance.Id] = None): Unit = {
    if (toKill.nonEmpty) {
      val dequeued = toKill.dequeue()
      async {
        await(instanceTracker.get(dequeued)) match {
          case None =>
            logger.warn(s"Deployment $deploymentId: Was about to kill instance $dequeued but it did not exist in the instance tracker anymore.")
          case Some(nextOldInstance) =>
            maybeNewInstanceId match {
              case Some(newInstanceId: Instance.Id) =>
                logger.info(s"Deployment $deploymentId: Killing old ${nextOldInstance.instanceId} because $newInstanceId became reachable")
              case _ =>
                logger.info(s"Deployment $deploymentId: Killing old ${nextOldInstance.instanceId}")
            }

            val goal = if (runSpec.isResident) Goal.Stopped else Goal.Decommissioned
            await(instanceTracker.setGoal(nextOldInstance.instanceId, goal, GoalChangeReason.Upgrading))
        }
      }
    }
  }

  def checkFinished(nr_new: Int, nr_old: Int): Unit = {
    if (nr_new == runSpec.instances && nr_old == 0) {
      logger.info(s"Deployment $deploymentId: All new instances for $pathId are ready and all old instances have been killed")
      promise.trySuccess(())
      context.stop(self)
    } else {
      logger.info(s"Deployment $deploymentId: For run spec: [${runSpec.id}] there are " +
        s"[${nr_new}] ready new instances and " +
        s"[${nr_old}] old instances. " +
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
  private[impl] case class RestartStrategy(nrToKillImmediately: Int, nrToStartImmediately: Int)

  private[impl] def computeRestartStrategy(runSpec: RunSpec, new_running: Int, old_running: Int, new_failing: Int, old_failing: Int): RestartStrategy = {
    val consideredHealthyInstancesCount = new_running + old_running
    val consideredUnhealthyInstancesCount = new_failing + old_failing
    val totalInstancesRunning = consideredHealthyInstancesCount + consideredUnhealthyInstancesCount

    // in addition to a spec which passed validation, we require:
    require(runSpec.instances > 0, s"target instance number must be > 0 but is ${runSpec.instances}")
    require(totalInstancesRunning >= 0, "current instances count must be >=0")

    // Old and new instances that have the Goal.Running & are considered healthy
    require(consideredHealthyInstancesCount >= 0, s"running instances count must be >=0 but is $consideredHealthyInstancesCount")

    val minHealthy = (runSpec.instances * runSpec.upgradeStrategy.minimumHealthCapacity).ceil.toInt
    var maxCapacity = (runSpec.instances * (1 + runSpec.upgradeStrategy.maximumOverCapacity)).toInt
    var nrToKillImmediately = math.max(0, consideredHealthyInstancesCount - minHealthy)

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
    if (runSpec.instances < new_running + old_running + old_failing) {
      // NOTE: We don't take into account the new app that are failing to count this
      // This is to avoid killing apps started but not ready
      nrToKillImmediately = math.max(new_running + old_running + old_failing - runSpec.instances, nrToKillImmediately)
      logger.info(s"runSpec.instances < currentInstances: Allowing killing all $nrToKillImmediately extra-instances")
    }

    logger.info(s"For minimumHealthCapacity ${runSpec.upgradeStrategy.minimumHealthCapacity} of ${runSpec.id.toString} leave " +
      s"$minHealthy instances running, maximum capacity $maxCapacity, killing $nrToKillImmediately of " +
      s"$consideredHealthyInstancesCount running instances immediately. (RunSpec version ${runSpec.version})")

    assume(nrToKillImmediately >= 0, s"nrToKillImmediately must be >=0 but is $nrToKillImmediately")
    assume(maxCapacity > 0, s"maxCapacity must be >0 but is $maxCapacity")
    def canStartNewInstances: Boolean = minHealthy < maxCapacity || consideredHealthyInstancesCount - nrToKillImmediately < maxCapacity
    assume(canStartNewInstances, "must be able to start new instances")

    val leftCapacity = math.max(0, maxCapacity - totalInstancesRunning)
    val instancesNotStartedYet = math.max(0, runSpec.instances - new_running - new_failing)
    val nrToStartImmediately = math.min(instancesNotStartedYet, leftCapacity)
    RestartStrategy(nrToKillImmediately = nrToKillImmediately, nrToStartImmediately = nrToStartImmediately)
  }
}

