package mesosphere.marathon
package raml

import scala.concurrent.duration._

trait ReadinessConversions {

  implicit val readinessProtocolWrites: Writes[core.readiness.ReadinessCheck.Protocol, HttpScheme] = Writes {
    case core.readiness.ReadinessCheck.Protocol.HTTP => HttpScheme.Http
    case core.readiness.ReadinessCheck.Protocol.HTTPS => HttpScheme.Https
  }

  implicit val readinessCheckWrites: Writes[core.readiness.ReadinessCheck, ReadinessCheck] = Writes { check =>
    ReadinessCheck(
      name = check.name,
      protocol = check.protocol.toRaml,
      path = check.path,
      portName = check.portName,
      intervalSeconds = check.interval.toSeconds.toInt,
      timeoutSeconds = check.timeout.toSeconds.toInt,
      httpStatusCodesForReady = check.httpStatusCodesForReady,
      preserveLastResponse = check.preserveLastResponse
    )
  }

  implicit val appReadinessRamlReader = Reads[ReadinessCheck, core.readiness.ReadinessCheck] { check =>
    import core.readiness.ReadinessCheck.Protocol._
    val protocol = check.protocol match {
      case HttpScheme.Http => HTTP
      case HttpScheme.Https => HTTPS
    }
    core.readiness.ReadinessCheck(
      name = check.name,
      protocol = protocol,
      path = check.path,
      portName = check.portName,
      interval = check.intervalSeconds.seconds,
      timeout = check.timeoutSeconds.seconds,
      httpStatusCodesForReady = check.httpStatusCodesForReady,
      preserveLastResponse = check.preserveLastResponse
    )
  }
}
