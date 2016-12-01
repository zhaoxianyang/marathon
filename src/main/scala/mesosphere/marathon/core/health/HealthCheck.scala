package mesosphere.marathon
package core.health

import com.wix.accord._
import mesosphere.marathon.Protos.HealthCheckDefinition.Protocol
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.state._
import org.apache.mesos.{ Protos => MesosProtos }

import scala.concurrent.duration._

sealed trait HealthCheck {
  def gracePeriod: FiniteDuration
  def interval: FiniteDuration
  def timeout: FiniteDuration
  def maxConsecutiveFailures: Int
  def toProto: Protos.HealthCheckDefinition

  protected def protoBuilder: Protos.HealthCheckDefinition.Builder =
    Protos.HealthCheckDefinition.newBuilder
      .setGracePeriodSeconds(this.gracePeriod.toSeconds.toInt)
      .setIntervalSeconds(this.interval.toSeconds.toInt)
      .setTimeoutSeconds(this.timeout.toSeconds.toInt)
      .setMaxConsecutiveFailures(this.maxConsecutiveFailures)
}

sealed trait PortReference extends Product with Serializable {
  def apply(assignments: Seq[PortAssignment]): PortAssignment
  def buildProto(builder: Protos.HealthCheckDefinition.Builder): Unit
}

object PortReference {
  case class ByIndex(value: Int) extends PortReference {
    override def apply(assignments: Seq[PortAssignment]): PortAssignment =
      assignments(value)
    override def buildProto(builder: Protos.HealthCheckDefinition.Builder): Unit =
      builder.setPortIndex(value)
  }

  case class ByName(value: String) extends PortReference {
    override def apply(assignments: Seq[PortAssignment]): PortAssignment =
      assignments.find(_.portName.contains(value)).getOrElse(
        throw new IndexOutOfBoundsException(s"no PortAssignment named $value")
      )
    override def buildProto(builder: Protos.HealthCheckDefinition.Builder): Unit =
      builder.setPortName(value)
  }

  def apply(value: Int): PortReference = ByIndex(value)
  def apply(value: String): PortReference = ByName(value)

  def fromProto(pb: Protos.HealthCheckDefinition): Option[PortReference] =
    if (pb.hasPortIndex) Some(ByIndex(pb.getPortIndex))
    else if (pb.hasPortName) Some(ByName(pb.getPortName))
    else if (!pb.hasPort) Some(ByIndex(0)) // backward compatibility, this used to be the default value in marathon.proto
    else None
}

sealed trait HealthCheckWithPort extends HealthCheck {
  def portIndex: Option[PortReference]
  def port: Option[Int]
}

object HealthCheckWithPort {
  val DefaultPortIndex = None
  val DefaultPort = None

  import mesosphere.marathon.api.v2.Validation.isTrue

  implicit val Validator: Validator[HealthCheckWithPort] =
    isTrue("HealthCheck must specify either a port or a portIndex") { hc =>
      hc.portIndex.isDefined ^ hc.port.isDefined
    }
}

sealed trait MarathonHealthCheck extends HealthCheckWithPort { this: HealthCheck =>
  def portIndex: Option[PortReference]
  def port: Option[Int]

  @SuppressWarnings(Array("OptionGet"))
  def effectivePort(app: AppDefinition, instance: Instance): Int = {
    // HealthChecks are only supported for legacy App instances with exactly one task
    val task = instance.firstTask
    def portViaIndex: Option[Int] = portIndex.map(_(task.status.networkInfo.portAssignments(app)).effectivePort)
    port.orElse(portViaIndex).get
  }
}

sealed trait MesosHealthCheck extends HealthCheck {
  def gracePeriod: FiniteDuration
  def interval: FiniteDuration
  def timeout: FiniteDuration
  def maxConsecutiveFailures: Int
  def delay: FiniteDuration

  override protected def protoBuilder: Protos.HealthCheckDefinition.Builder =
    super.protoBuilder.setDelaySeconds(delay.toSeconds.toInt)

  def toMesos(portAssignments: Seq[PortAssignment] = Seq.empty): MesosProtos.HealthCheck
}

sealed trait MesosHealthCheckWithPorts extends HealthCheckWithPort { this: HealthCheck =>
  @SuppressWarnings(Array("OptionGet"))
  def effectivePort(portAssignments: Seq[PortAssignment]): Int = {
    port.getOrElse {
      val portAssignment: Option[PortAssignment] = portIndex.flatMap {
        case intIndex: PortReference.ByIndex => Some(portAssignments(intIndex.value))
        case nameIndex: PortReference.ByName => portAssignments.find(_.portName.contains(nameIndex.value))
      }
      portAssignment.flatMap(_.containerPort).getOrElse(portAssignment.flatMap(_.hostPort).get)
    }
  }
}

case class MarathonHttpHealthCheck(
  gracePeriod: FiniteDuration = HealthCheck.DefaultGracePeriod,
  interval: FiniteDuration = HealthCheck.DefaultInterval,
  timeout: FiniteDuration = HealthCheck.DefaultTimeout,
  maxConsecutiveFailures: Int = HealthCheck.DefaultMaxConsecutiveFailures,
  portIndex: Option[PortReference] = HealthCheckWithPort.DefaultPortIndex,
  port: Option[Int] = HealthCheckWithPort.DefaultPort,
  path: Option[String] = MarathonHttpHealthCheck.DefaultPath,
  protocol: Protocol = MarathonHttpHealthCheck.DefaultProtocol,
  ignoreHttp1xx: Boolean = MarathonHttpHealthCheck.DefaultIgnoreHttp1xx)
    extends HealthCheck with MarathonHealthCheck {
  override def toProto: Protos.HealthCheckDefinition = {
    val builder = protoBuilder
      .setProtocol(protocol)
      .setIgnoreHttp1Xx(this.ignoreHttp1xx)

    path.foreach(builder.setPath)
    portIndex.foreach(_.buildProto(builder))
    port.foreach(builder.setPort)

    builder.build
  }
}

object MarathonHttpHealthCheck {
  val DefaultPath = None
  val DefaultIgnoreHttp1xx = false
  val DefaultProtocol = Protocol.HTTP

  def mergeFromProto(proto: Protos.HealthCheckDefinition): MarathonHttpHealthCheck =
    MarathonHttpHealthCheck(
      gracePeriod = proto.getGracePeriodSeconds.seconds,
      timeout = proto.getTimeoutSeconds.seconds,
      interval = proto.getIntervalSeconds.seconds,
      maxConsecutiveFailures = proto.getMaxConsecutiveFailures,
      ignoreHttp1xx = proto.getIgnoreHttp1Xx,
      path = if (proto.hasPath) Some(proto.getPath) else None,
      portIndex = PortReference.fromProto(proto),
      port = if (proto.hasPort) Some(proto.getPort) else None,
      protocol = proto.getProtocol
    )
}

case class MarathonTcpHealthCheck(
  gracePeriod: FiniteDuration = HealthCheck.DefaultGracePeriod,
  interval: FiniteDuration = HealthCheck.DefaultInterval,
  timeout: FiniteDuration = HealthCheck.DefaultTimeout,
  maxConsecutiveFailures: Int = HealthCheck.DefaultMaxConsecutiveFailures,
  portIndex: Option[PortReference] = HealthCheckWithPort.DefaultPortIndex,
  port: Option[Int] = HealthCheckWithPort.DefaultPort)
    extends HealthCheck with MarathonHealthCheck {
  override def toProto: Protos.HealthCheckDefinition = {
    val builder = protoBuilder.setProtocol(Protos.HealthCheckDefinition.Protocol.TCP)

    portIndex.foreach(_.buildProto(builder))
    port.foreach(builder.setPort)

    builder.build
  }
}

object MarathonTcpHealthCheck {
  def mergeFromProto(proto: Protos.HealthCheckDefinition): MarathonTcpHealthCheck =
    MarathonTcpHealthCheck(
      gracePeriod = proto.getGracePeriodSeconds.seconds,
      timeout = proto.getTimeoutSeconds.seconds,
      interval = proto.getIntervalSeconds.seconds,
      maxConsecutiveFailures = proto.getMaxConsecutiveFailures,
      portIndex = PortReference.fromProto(proto),
      port = if (proto.hasPort) Some(proto.getPort) else None
    )
}

case class MesosCommandHealthCheck(
  gracePeriod: FiniteDuration = HealthCheck.DefaultGracePeriod,
  interval: FiniteDuration = HealthCheck.DefaultInterval,
  timeout: FiniteDuration = HealthCheck.DefaultTimeout,
  maxConsecutiveFailures: Int = HealthCheck.DefaultMaxConsecutiveFailures,
  delay: FiniteDuration = HealthCheck.DefaultDelay,
  command: Executable)
    extends HealthCheck with MesosHealthCheck {
  override def toProto: Protos.HealthCheckDefinition = {
    protoBuilder
      .setProtocol(Protos.HealthCheckDefinition.Protocol.COMMAND)
      .setCommand(Executable.toProto(command))
      .build
  }

  def toMesos(portAssignments: Seq[PortAssignment] = Seq.empty): MesosProtos.HealthCheck = {
    MesosProtos.HealthCheck.newBuilder
      .setType(MesosProtos.HealthCheck.Type.COMMAND)
      .setIntervalSeconds(this.interval.toSeconds.toDouble)
      .setTimeoutSeconds(this.timeout.toSeconds.toDouble)
      .setConsecutiveFailures(this.maxConsecutiveFailures)
      .setGracePeriodSeconds(this.gracePeriod.toUnit(SECONDS))
      .setDelaySeconds(this.delay.toUnit(SECONDS))
      .setCommand(Executable.toProto(this.command))
      .build()
  }
}

object MesosCommandHealthCheck {
  def mergeFromProto(proto: Protos.HealthCheckDefinition): MesosCommandHealthCheck =
    MesosCommandHealthCheck(
      gracePeriod = proto.getGracePeriodSeconds.seconds,
      timeout = proto.getTimeoutSeconds.seconds,
      interval = proto.getIntervalSeconds.seconds,
      maxConsecutiveFailures = proto.getMaxConsecutiveFailures,
      delay = proto.getDelaySeconds.seconds,
      command = Executable.mergeFromProto(proto.getCommand)
    )
}

case class MesosHttpHealthCheck(
  gracePeriod: FiniteDuration = HealthCheck.DefaultGracePeriod,
  interval: FiniteDuration = HealthCheck.DefaultInterval,
  timeout: FiniteDuration = HealthCheck.DefaultTimeout,
  maxConsecutiveFailures: Int = HealthCheck.DefaultMaxConsecutiveFailures,
  portIndex: Option[PortReference] = HealthCheckWithPort.DefaultPortIndex,
  port: Option[Int] = HealthCheckWithPort.DefaultPort,
  path: Option[String] = MarathonHttpHealthCheck.DefaultPath,
  protocol: Protocol = MesosHttpHealthCheck.DefaultProtocol,
  delay: FiniteDuration = HealthCheck.DefaultDelay)
    extends HealthCheck with MesosHealthCheck with MesosHealthCheckWithPorts {
  require(protocol == Protocol.MESOS_HTTP || protocol == Protocol.MESOS_HTTPS)

  override def toProto: Protos.HealthCheckDefinition = {
    val builder = protoBuilder
      .setProtocol(protocol)

    path.foreach(builder.setPath)

    portIndex.foreach(_.buildProto(builder))
    port.foreach(builder.setPort)

    builder.build
  }

  override def toMesos(portAssignments: Seq[PortAssignment] = Seq.empty): MesosProtos.HealthCheck = {
    val httpInfoBuilder = MesosProtos.HealthCheck.HTTPCheckInfo.newBuilder()
      .setScheme(if (protocol == Protocol.MESOS_HTTP) "http" else "https")
      .setPort(effectivePort(portAssignments))
    path.foreach(httpInfoBuilder.setPath)

    MesosProtos.HealthCheck.newBuilder
      .setType(MesosProtos.HealthCheck.Type.HTTP)
      .setIntervalSeconds(this.interval.toSeconds.toDouble)
      .setTimeoutSeconds(this.timeout.toSeconds.toDouble)
      .setConsecutiveFailures(this.maxConsecutiveFailures)
      .setGracePeriodSeconds(this.gracePeriod.toUnit(SECONDS))
      .setDelaySeconds(this.delay.toUnit(SECONDS))
      .setHttp(httpInfoBuilder)
      .build()
  }
}

object MesosHttpHealthCheck {
  val DefaultPath = None
  val DefaultProtocol = Protocol.MESOS_HTTP

  def mergeFromProto(proto: Protos.HealthCheckDefinition): MesosHttpHealthCheck =
    MesosHttpHealthCheck(
      gracePeriod = proto.getGracePeriodSeconds.seconds,
      timeout = proto.getTimeoutSeconds.seconds,
      interval = proto.getIntervalSeconds.seconds,
      maxConsecutiveFailures = proto.getMaxConsecutiveFailures,
      delay = proto.getDelaySeconds.seconds,
      path = if (proto.hasPath) Some(proto.getPath) else None,
      portIndex = PortReference.fromProto(proto),
      port = if (proto.hasPort) Some(proto.getPort) else None,
      protocol = proto.getProtocol
    )
}

case class MesosTcpHealthCheck(
  gracePeriod: FiniteDuration = HealthCheck.DefaultGracePeriod,
  interval: FiniteDuration = HealthCheck.DefaultInterval,
  timeout: FiniteDuration = HealthCheck.DefaultTimeout,
  maxConsecutiveFailures: Int = HealthCheck.DefaultMaxConsecutiveFailures,
  portIndex: Option[PortReference] = HealthCheckWithPort.DefaultPortIndex,
  port: Option[Int] = HealthCheckWithPort.DefaultPort,
  delay: FiniteDuration = HealthCheck.DefaultDelay)
    extends HealthCheck with MesosHealthCheck with MesosHealthCheckWithPorts {
  override def toProto: Protos.HealthCheckDefinition = {
    val builder = protoBuilder.setProtocol(Protos.HealthCheckDefinition.Protocol.MESOS_TCP)

    portIndex.foreach(_.buildProto(builder))
    port.foreach(builder.setPort)

    builder.build
  }

  override def toMesos(portAssignments: Seq[PortAssignment] = Seq.empty): MesosProtos.HealthCheck = {
    val tcpInfoBuilder = MesosProtos.HealthCheck.TCPCheckInfo.newBuilder().setPort(effectivePort(portAssignments))

    MesosProtos.HealthCheck.newBuilder
      .setType(MesosProtos.HealthCheck.Type.TCP)
      .setIntervalSeconds(this.interval.toSeconds.toDouble)
      .setTimeoutSeconds(this.timeout.toSeconds.toDouble)
      .setConsecutiveFailures(this.maxConsecutiveFailures)
      .setGracePeriodSeconds(this.gracePeriod.toUnit(SECONDS))
      .setDelaySeconds(this.delay.toUnit(SECONDS))
      .setTcp(tcpInfoBuilder)
      .build()
  }
}

object MesosTcpHealthCheck {
  def mergeFromProto(proto: Protos.HealthCheckDefinition): MesosTcpHealthCheck =
    MesosTcpHealthCheck(
      gracePeriod = proto.getGracePeriodSeconds.seconds,
      timeout = proto.getTimeoutSeconds.seconds,
      interval = proto.getIntervalSeconds.seconds,
      maxConsecutiveFailures = proto.getMaxConsecutiveFailures,
      delay = proto.getDelaySeconds.seconds,
      portIndex = PortReference.fromProto(proto),
      port = if (proto.hasPort) Some(proto.getPort) else None
    )
}

object HealthCheck {
  val DefaultProtocol = Protocol.HTTP
  // Docker images can take a long time to download, so default to a fairly long wait.
  val DefaultGracePeriod = 5.minutes
  val DefaultInterval = 1.minute
  val DefaultTimeout = 20.seconds
  val DefaultMaxConsecutiveFailures = 3
  // As soon as an application is started (before a task is launched), we start the health checks for this app.
  // We optimistically set a low value here, for tasks that start really fast
  val DefaultFirstHealthCheckAfter = 5.seconds
  val DefaultDelay = 15.seconds

  implicit val Validator: Validator[HealthCheck] = new Validator[HealthCheck] {
    override def apply(hc: HealthCheck): Result = {
      hc match {
        case h: HealthCheckWithPort => HealthCheckWithPort.Validator(h)
        case _ => Success
      }
    }
  }

  def fromProto(proto: Protos.HealthCheckDefinition): HealthCheck = {
    proto.getProtocol match {
      case Protocol.COMMAND => MesosCommandHealthCheck.mergeFromProto(proto)
      case Protocol.TCP => MarathonTcpHealthCheck.mergeFromProto(proto)
      case Protocol.HTTP | Protocol.HTTPS => MarathonHttpHealthCheck.mergeFromProto(proto)
      case Protocol.MESOS_TCP => MesosTcpHealthCheck.mergeFromProto(proto)
      case Protocol.MESOS_HTTP | Protocol.MESOS_HTTPS => MesosHttpHealthCheck.mergeFromProto(proto)
    }
  }
}
