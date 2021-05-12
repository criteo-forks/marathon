package mesosphere.marathon
package api.v2

import javax.inject.Inject
import mesosphere.marathon.api.AuthResource
import mesosphere.marathon.core.group.GroupManager
import mesosphere.marathon.core.health.AntiSnowballApi
import mesosphere.marathon.plugin.auth.{Authenticator, Authorizer, ViewRunSpec}
import mesosphere.marathon.raml.Raml
import mesosphere.marathon.state.PathId._
import mesosphere.marathon.state.Timestamp

import javax.servlet.http.HttpServletRequest
import javax.ws.rs._
import javax.ws.rs.container.{AsyncResponse, Suspended}
import javax.ws.rs.core.{Context, MediaType}
import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext

@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
class AppAntiSnowballResource(
    service: AntiSnowballApi,
    groupManager: GroupManager,
    val authenticator: Authenticator,
    val authorizer: Authorizer,
    val config: MarathonConf)(implicit val executionContext: ExecutionContext) extends AuthResource {

  @GET
  def index(
    @PathParam("appId") appId: String,
    @Context req: HttpServletRequest,
    @Suspended asyncResponse: AsyncResponse): Unit = sendResponse(asyncResponse) {
    async {
      implicit val identity = await(authenticatedAsync(req))
      val id = appId.toAbsolutePath
      withAuthorization(ViewRunSpec, groupManager.app(id), unknownApp(id)) { _ =>
        val status = service.getStatus(id)
        ok(Raml.toRaml(status))
      }
    }
  }
}
