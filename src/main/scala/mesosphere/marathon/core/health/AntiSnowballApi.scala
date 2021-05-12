package mesosphere.marathon
package core.health

import mesosphere.marathon.core.task.Task
import mesosphere.marathon.state.{AbsolutePathId, Timestamp}

import scala.concurrent.duration._
import scala.concurrent.Future
import akka.Done
import mesosphere.marathon.core.health.impl.AntiSnowballStatus

import javax.inject.Inject

trait AntiSnowballApi {

  def getStatus(appId: AbsolutePathId): AntiSnowballStatus

  def setActive(appId: AbsolutePathId, active: Boolean)
}
