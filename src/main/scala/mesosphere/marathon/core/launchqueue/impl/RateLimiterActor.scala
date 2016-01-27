package mesosphere.marathon.core.launchqueue.impl

import akka.actor.{ Actor, ActorLogging, ActorRef, Cancellable, Props }
import akka.event.LoggingReceive
import mesosphere.marathon.core.launchqueue.impl.RateLimiterActor.{
  AddDelay,
  ResetViableTasksDelays,
  DecreaseDelay,
  DelayUpdate,
  GetDelay,
  ResetDelay,
  ResetDelayResponse
}
import mesosphere.marathon.state.{ AppDefinition, AppRepository, Timestamp }
import mesosphere.marathon.tasks.TaskTracker

import scala.concurrent.duration._

private[launchqueue] object RateLimiterActor {
  def props(
    rateLimiter: RateLimiter,
    taskTracker: TaskTracker,
    appRepository: AppRepository,
    launchQueueRef: ActorRef): Props =
    Props(new RateLimiterActor(
      rateLimiter, taskTracker, appRepository, launchQueueRef
    ))

  case class DelayUpdate(app: AppDefinition, delayUntil: Timestamp)

  case class ResetDelay(app: AppDefinition)
  case object ResetDelayResponse

  case class GetDelay(appDefinition: AppDefinition)
  private[impl] case class AddDelay(app: AppDefinition)
  private[impl] case class DecreaseDelay(app: AppDefinition)

  private case object ResetViableTasksDelays
}

private class RateLimiterActor private (
    rateLimiter: RateLimiter,
    taskTracker: TaskTracker,
    appRepository: AppRepository,
    launchQueueRef: ActorRef) extends Actor with ActorLogging {
  var cleanup: Cancellable = _

  override def preStart(): Unit = {
    import context.dispatcher
    cleanup = context.system.scheduler.schedule(10.seconds, 10.seconds, self, ResetViableTasksDelays)
    log.info("started RateLimiterActor")
  }

  override def postStop(): Unit = {
    cleanup.cancel()
  }

  override def receive: Receive = LoggingReceive {
    Seq[Receive](
      receiveCleanup,
      receiveDelayOps
    ).reduceLeft(_.orElse[Any, Unit](_))
  }

  /**
    * If an app gets removed or updated, the delay should be reset. If
    * an app is considered viable, the delay should be reset too. We
    * check and reset viable tasks' delays periodically.
    */
  private[this] def receiveCleanup: Receive = {
    case ResetViableTasksDelays =>
      rateLimiter.resetViableTasksDelays()
  }

  private[this] def receiveDelayOps: Receive = {
    case GetDelay(app) =>
      sender() ! DelayUpdate(app, rateLimiter.getDeadline(app))

    case AddDelay(app) =>
      rateLimiter.addDelay(app)
      launchQueueRef ! DelayUpdate(app, rateLimiter.getDeadline(app))

    case DecreaseDelay(app) => // ignore for now

    case ResetDelay(app) =>
      rateLimiter.resetDelay(app)
      launchQueueRef ! DelayUpdate(app, rateLimiter.getDeadline(app))
      sender() ! ResetDelayResponse
  }
}
