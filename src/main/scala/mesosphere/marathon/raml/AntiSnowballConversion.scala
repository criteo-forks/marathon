package mesosphere.marathon
package raml

import mesosphere.marathon.core.health.impl.AntiSnowballStatus

trait AntiSnowballConversion {
  implicit val antiSnowballStatusWrite: Writes[mesosphere.marathon.core.health.impl.AntiSnowballStatus, raml.AntiSnowballStatus] = Writes { status =>
    AntiSnowballStatus(active = status.active)
  }
}
