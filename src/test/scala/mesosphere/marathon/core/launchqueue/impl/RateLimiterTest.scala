package mesosphere.marathon.core.launchqueue.impl

import akka.actor.ActorSystem
import akka.testkit.TestKit
import mesosphere.marathon.MarathonSpec
import mesosphere.marathon.core.base.ConstantClock
import mesosphere.marathon.state.{ Timestamp, AppDefinition }
import mesosphere.marathon.state.PathId._
import org.scalatest.Matchers

import scala.concurrent.duration._

class RateLimiterTest extends TestKit(ActorSystem("system")) with MarathonSpec with Matchers {
  val clock = ConstantClock(Timestamp.now())

  test("addDelay") {
    val limiter = new RateLimiter(clock)
    val app = AppDefinition(id = "test".toPath, backoff = 10.seconds)

    limiter.addDelay(app)

    limiter.getDeadline(app) should be(clock.now() + 10.seconds)
  }

  test("addDelay for existing delay") {
    val limiter = new RateLimiter(clock)
    val app = AppDefinition(id = "test".toPath, backoff = 10.seconds, backoffFactor = 2.0)

    limiter.addDelay(app)
    limiter.addDelay(app)

    limiter.getDeadline(app) should be(clock.now() + 20.seconds)
  }

  test("resetViableTasksDelays") {
    val time_origin = clock.now()
    val limiter = new RateLimiter(clock)
    val threshold = limiter.getMinimumTaskExecutionSeconds
    val viable = AppDefinition(id = "viable".toPath, backoff = 10.seconds)
    limiter.addDelay(viable)
    val notYetViable = AppDefinition(id = "notYetViable".toPath, backoff = 20.seconds)
    limiter.addDelay(notYetViable)
    val stillWaiting = AppDefinition(id = "test".toPath, backoff = (threshold + 20).seconds)
    limiter.addDelay(stillWaiting)

    clock += (threshold + 11).seconds

    limiter.resetViableTasksDelays()

    limiter.getDeadline(viable) should be(clock.now())
    limiter.getDeadline(notYetViable) should be(time_origin + 20.seconds)
    limiter.getDeadline(stillWaiting) should be(time_origin + (threshold + 20).seconds)
  }

  test("resetDelay") {
    val limiter = new RateLimiter(clock)
    val app = AppDefinition(id = "test".toPath, backoff = 10.seconds)

    limiter.addDelay(app)

    limiter.resetDelay(app)

    limiter.getDeadline(app) should be(clock.now())
  }

}
