package mesosphere.marathon
package core.health.impl

import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.api.v2.json.Formats._
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.state.{AbsolutePathId, Timestamp}
import mesosphere.marathon.core.health.{AntiSnowballApi, HealthCheckShield, HealthCheckShieldApi, HealthCheckShieldConf}

import scala.collection.concurrent.TrieMap
import scala.async.Async.{async, await}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.ActorMaterializer

import scala.concurrent.Future
import akka.Done

import scala.concurrent.ExecutionContext

/* Store info about anti-snowball status for each app
*/
final class AntiSnowballApiImpl()(implicit ec: ExecutionContext, mat: ActorMaterializer)
  extends AntiSnowballApi with StrictLogging {

  // maybe we'll eventually store a AntiSnowballStatus object
  private val antiSnowballStatus = TrieMap.empty[AbsolutePathId, Boolean]

  def getStatus(appId: AbsolutePathId): AntiSnowballStatus = {
    val active = antiSnowballStatus.getOrElse(appId, false)
    new AntiSnowballStatus(active)
  }

  def setActive(appId: AbsolutePathId, active: Boolean): Unit = {
    antiSnowballStatus.update(appId, active)
  }
}

class AntiSnowballStatus(val active: Boolean) {

}
