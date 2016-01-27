package mesosphere.marathon.core.launchqueue.impl

import java.util.concurrent.TimeUnit

import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.state.{ AppDefinition, PathId, Timestamp }
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

/**
  * Manages the task launch delays for every app and config version.
  *
  * We do not keep the delays for every version because that would include scaling changes or manual restarts.
  */
private[launchqueue] class RateLimiter(clock: Clock) {
  import RateLimiter._

  /** The task launch delays per app and their last config change. */
  private[this] var taskLaunchDelays = Map[(PathId, Timestamp), Delay]()

  def getMinimumTaskExecutionSeconds: Long = minimumTaskExecutionSeconds

  /**
    * Reset delay for tasks that have reached the viability
    * threshold. The deadline indicates when the task has been
    * launched for the last time.
    */
  def resetViableTasksDelays(): Unit = {
    taskLaunchDelays = taskLaunchDelays.filter {
      case (_, delay) =>
        clock.now() - FiniteDuration(minimumTaskExecutionSeconds, TimeUnit.SECONDS) < delay.deadline
    }
  }

  def getDeadline(app: AppDefinition): Timestamp =
    taskLaunchDelays.get(app.id -> app.versionInfo.lastConfigChangeVersion).map(_.deadline) getOrElse clock.now()

  def addDelay(app: AppDefinition): Timestamp = {
    setNewDelay(app, "Increasing delay") {
      case Some(delay) => Some(delay.increased(clock, app))
      case None        => Some(Delay(clock, app))
    }
  }

  private[this] def setNewDelay(app: AppDefinition, message: String)(
    calcDelay: Option[Delay] => Option[Delay]): Timestamp = {
    val maybeDelay: Option[Delay] = taskLaunchDelays.get(app.id -> app.versionInfo.lastConfigChangeVersion)
    calcDelay(maybeDelay) match {
      case Some(newDelay) =>
        import mesosphere.util.DurationToHumanReadable
        val now: Timestamp = clock.now()
        val priorTimeLeft = (now until maybeDelay.map(_.deadline).getOrElse(now)).toHumanReadable
        val timeLeft = (now until newDelay.deadline).toHumanReadable

        if (newDelay.deadline <= now) {
          resetDelay(app)
        }
        else {
          log.info(s"$message. Task launch delay for [${app.id}] changed from [$priorTimeLeft] to [$timeLeft].")
          taskLaunchDelays += ((app.id, app.versionInfo.lastConfigChangeVersion) -> newDelay)
        }
        newDelay.deadline

      case None =>
        resetDelay(app)
        clock.now()
    }
  }

  def resetDelay(app: AppDefinition): Unit = {
    if (taskLaunchDelays contains (app.id -> app.versionInfo.lastConfigChangeVersion)) {
      log.info(s"Task launch delay for [${app.id} - ${app.versionInfo.lastConfigChangeVersion}}] reset to zero")
      taskLaunchDelays -= (app.id -> app.versionInfo.lastConfigChangeVersion)
    }
  }
}

private object RateLimiter {
  /**
    * The viability threshold may be tuned later, once the mechanism
    * has been validated.
    */
  private val minimumTaskExecutionSeconds: Long = 60

  private val log = LoggerFactory.getLogger(getClass.getName)

  private object Delay {
    def apply(clock: Clock, app: AppDefinition): Delay = Delay(clock.now() + app.backoff, app.backoff)
    def apply(clock: Clock, delay: FiniteDuration): Delay = Delay(clock.now() + delay, delay)
  }

  private case class Delay(
      deadline: Timestamp,
      delay: FiniteDuration) {

    def increased(clock: Clock, app: AppDefinition): Delay = {
      val newDelay: FiniteDuration =
        app.maxLaunchDelay min FiniteDuration((delay.toNanos * app.backoffFactor).toLong, TimeUnit.NANOSECONDS)
      Delay(clock, newDelay)
    }
  }
}
