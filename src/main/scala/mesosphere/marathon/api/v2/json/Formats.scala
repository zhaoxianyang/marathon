package mesosphere.marathon
package api.v2.json

import mesosphere.marathon.Protos.Constraint
import mesosphere.marathon.Protos.Constraint.Operator
import mesosphere.marathon.Protos.HealthCheckDefinition.Protocol
import mesosphere.marathon.Protos.ResidencyDefinition.TaskLostBehavior
import mesosphere.marathon.core.appinfo._
import mesosphere.marathon.core.event._
import mesosphere.marathon.core.health._
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.plugin.{ PluginDefinition, PluginDefinitions }
import mesosphere.marathon.core.pod.PodDefinition
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.raml.{ App, Pod, Raml }
import mesosphere.marathon.state._
import mesosphere.marathon.upgrade.DeploymentManager.DeploymentStepInfo
import mesosphere.marathon.upgrade._
import org.apache.mesos.{ Protos => mesos }
import play.api.data.validation.ValidationError
import play.api.libs.functional.syntax._
import play.api.libs.json._

import scala.collection.immutable.Seq
import scala.concurrent.duration._

// TODO: We should replace this entire thing with the auto-generated formats from the RAML.
// See https://mesosphere.atlassian.net/browse/MARATHON-1291
// https://mesosphere.atlassian.net/browse/MARATHON-1292
object Formats extends Formats {

  implicit class ReadsWithDefault[A](val reads: Reads[Option[A]]) extends AnyVal {
    def withDefault(a: A): Reads[A] = reads.map(_.getOrElse(a))
  }

  implicit class FormatWithDefault[A](val m: OFormat[Option[A]]) extends AnyVal {
    def withDefault(a: A): OFormat[A] = m.inmap(_.getOrElse(a), Some(_))
  }

  implicit class ReadsAsSeconds(val reads: Reads[Long]) extends AnyVal {
    def asSeconds: Reads[FiniteDuration] = reads.map(_.seconds)
  }

  implicit class FormatAsSeconds(val format: OFormat[Long]) extends AnyVal {
    def asSeconds: OFormat[FiniteDuration] =
      format.inmap(
        _.seconds,
        _.toSeconds
      )
  }
}

trait Formats
    extends AppAndGroupFormats
    with HealthCheckFormats
    with ReadinessCheckFormats
    with FetchUriFormats
    with ContainerFormats
    with DeploymentFormats
    with EventFormats
    with EventSubscribersFormats
    with PluginFormats
    with SecretFormats {

  implicit lazy val TaskFailureWrites: Writes[TaskFailure] = Writes { failure =>
    Json.obj(
      "appId" -> failure.appId,
      "host" -> failure.host,
      "message" -> failure.message,
      "state" -> failure.state.name(),
      "taskId" -> failure.taskId.getValue,
      "timestamp" -> failure.timestamp,
      "version" -> failure.version,
      "slaveId" -> failure.slaveId.fold[JsValue](JsNull){ slaveId => JsString(slaveId.getValue) }
    )
  }

  implicit lazy val networkInfoProtocolWrites = Writes[mesos.NetworkInfo.Protocol] { protocol =>
    JsString(protocol.name)
  }

  private[this] val allowedProtocolString =
    mesos.NetworkInfo.Protocol.values().toSeq.map(_.getDescriptorForType.getName).mkString(", ")

  implicit lazy val networkInfoProtocolReads = Reads[mesos.NetworkInfo.Protocol] { json =>
    json.validate[String].flatMap { protocolString: String =>

      Option(mesos.NetworkInfo.Protocol.valueOf(protocolString)) match {
        case Some(protocol) => JsSuccess(protocol)
        case None =>
          JsError(s"'$protocolString' is not a valid protocol. Allowed values: $allowedProtocolString")
      }

    }
  }

  implicit lazy val ipAddressFormat: Format[mesos.NetworkInfo.IPAddress] = {
    def toIpAddress(ipAddress: String, protocol: mesos.NetworkInfo.Protocol): mesos.NetworkInfo.IPAddress =
      mesos.NetworkInfo.IPAddress.newBuilder().setIpAddress(ipAddress).setProtocol(protocol).build()

    def toTuple(ipAddress: mesos.NetworkInfo.IPAddress): (String, mesos.NetworkInfo.Protocol) =
      (ipAddress.getIpAddress, ipAddress.getProtocol)

    (
      (__ \ "ipAddress").format[String] ~
      (__ \ "protocol").format[mesos.NetworkInfo.Protocol]
    )(toIpAddress, toTuple)
  }

  implicit lazy val InstanceIdWrite: Writes[Instance.Id] = Writes { id => JsString(id.idString) }
  implicit lazy val TaskStateFormat: Format[mesos.TaskState] =
    enumFormat(mesos.TaskState.valueOf, str => s"$str is not a valid TaskState type")

  implicit lazy val InstanceWrites: Writes[Instance] = Writes { instance =>
    Json.arr(instance.tasksMap.values.map(TaskWrites.writes(_).as[JsObject]))
  }

  implicit val TaskWrites: Writes[Task] = Writes { task =>
    val base = Json.obj(
      "id" -> task.taskId,
      "slaveId" -> task.agentInfo.agentId,
      "host" -> task.agentInfo.host,
      "state" -> task.status.condition.toReadableName
    )

    val launched = task.launched.map { launched =>
      task.status.ipAddresses.foldLeft(
        base ++ Json.obj (
          "startedAt" -> task.status.startedAt,
          "stagedAt" -> task.status.stagedAt,
          "ports" -> launched.hostPorts,
          "version" -> task.runSpecVersion
        )
      ){
          case (launchedJs, ipAddresses) => launchedJs ++ Json.obj("ipAddresses" -> ipAddresses)
        }
    }.getOrElse(base)

    val reservation = task.reservationWithVolumes.map { reservation =>
      launched ++ Json.obj(
        "localVolumes" -> reservation.volumeIds
      )
    }.getOrElse(launched)

    reservation
  }

  implicit lazy val EnrichedTaskWrites: Writes[EnrichedTask] = Writes { task =>
    val taskJson = TaskWrites.writes(task.task).as[JsObject]

    val enrichedJson = taskJson ++ Json.obj(
      "appId" -> task.appId
    )

    val withServicePorts = if (task.servicePorts.nonEmpty)
      enrichedJson ++ Json.obj("servicePorts" -> task.servicePorts)
    else
      enrichedJson

    if (task.healthCheckResults.nonEmpty)
      withServicePorts ++ Json.obj("healthCheckResults" -> task.healthCheckResults)
    else
      withServicePorts
  }

  implicit lazy val PathIdFormat: Format[PathId] = Format(
    Reads.of[String](Reads.minLength[String](1)).map(PathId(_)),
    Writes[PathId] { id => JsString(id.toString) }
  )

  implicit lazy val InstanceIdFormat: Format[Instance.Id] = Format(
    Reads.of[String](Reads.minLength[String](3)).map(Instance.Id(_)),
    Writes[Instance.Id] { id => JsString(id.idString) }
  )

  implicit lazy val TimestampFormat: Format[Timestamp] = Format(
    Reads.of[String].map(Timestamp(_)),
    Writes[Timestamp] { t => JsString(t.toString) }
  )

  implicit lazy val CommandFormat: Format[Command] = Json.format[Command]

  implicit lazy val ParameterFormat: Format[Parameter] = (
    (__ \ "key").format[String] ~
    (__ \ "value").format[String]
  )(Parameter(_, _), unlift(Parameter.unapply))

  /*
 * Helpers
 */

  def uniquePorts: Reads[Seq[Int]] = Format.of[Seq[Int]].filter(ValidationError("Ports must be unique.")) { ports =>
    val withoutRandom = ports.filterNot(_ == AppDefinition.RandomPortValue)
    withoutRandom.distinct.size == withoutRandom.size
  }

  def nonEmpty[C <: Iterable[_]](implicit reads: Reads[C]): Reads[C] =
    Reads.filterNot[C](ValidationError("set must not be empty"))(_.isEmpty)(reads)

  def enumFormat[A <: java.lang.Enum[A]](read: String => A, errorMsg: String => String): Format[A] = {
    val reads = Reads[A] {
      case JsString(str) =>
        try {
          JsSuccess(read(str))
        } catch {
          case _: IllegalArgumentException => JsError(errorMsg(str))
        }

      case x: JsValue => JsError(s"expected string, got $x")
    }

    val writes = Writes[A] { a: A => JsString(a.name) }

    Format(reads, writes)
  }
}

trait ContainerFormats {
  import Formats._

  implicit lazy val PortMappingFormat: Format[Container.PortMapping] = (
    (__ \ "containerPort").formatNullable[Int].withDefault(AppDefinition.RandomPortValue) ~
    (__ \ "hostPort").formatNullable[Int] ~
    (__ \ "servicePort").formatNullable[Int].withDefault(AppDefinition.RandomPortValue) ~
    (__ \ "protocol").formatNullable[String].withDefault("tcp") ~
    (__ \ "name").formatNullable[String] ~
    (__ \ "labels").formatNullable[Map[String, String]].withDefault(Map.empty[String, String])
  )(Container.PortMapping(_, _, _, _, _, _), unlift(Container.PortMapping.unapply))

  implicit lazy val CredentialFormat: Format[Container.Credential] = (
    (__ \ "principal").format[String] ~
    (__ \ "secret").formatNullable[String]
  )(Container.Credential.apply, unlift(Container.Credential.unapply))

  implicit lazy val ModeFormat: Format[mesos.Volume.Mode] =
    enumFormat(mesos.Volume.Mode.valueOf, str => s"$str is not a valid mode")

  implicit lazy val DiskTypeFormat = new Format[DiskType] {
    // override def
    override def reads(json: JsValue): JsResult[DiskType] = {
      json.asOpt[String] match {
        case None | Some("root") => JsSuccess(DiskType.Root)
        case Some("path") => JsSuccess(DiskType.Path)
        case Some("mount") => JsSuccess(DiskType.Mount)
        case Some(otherwise) =>
          JsError(s"No such disk type: $otherwise")
      }
    }
    override def writes(persistentVolumeType: DiskType): JsValue = JsString(
      persistentVolumeType match {
        case DiskType.Root => "root"
        case DiskType.Path => "path"
        case DiskType.Mount => "mount"
      }
    )
  }

  implicit lazy val PersistentVolumeInfoReader: Reads[PersistentVolumeInfo] =
    ((__ \ "size").read[Long] ~
      (__ \ "maxSize").readNullable[Long] ~
      (__ \ "type").readNullable[DiskType].withDefault(DiskType.Root) ~
      (__ \ "constraints").readNullable[Set[Constraint]].withDefault(Set.empty))(
        PersistentVolumeInfo(_, _, _, _))
  implicit lazy val PersistentVolumeInfoWriter: Writes[PersistentVolumeInfo] = Json.writes[PersistentVolumeInfo]

  implicit lazy val ExternalVolumeInfoFormat: Format[ExternalVolumeInfo] = (
    (__ \ "size").formatNullable[Long] ~
    (__ \ "name").format[String] ~
    (__ \ "provider").format[String] ~
    (__ \ "options").formatNullable[Map[String, String]].withDefault(Map.empty[String, String])
  )(ExternalVolumeInfo(_, _, _, _), unlift(ExternalVolumeInfo.unapply))

  implicit lazy val VolumeFormat: Format[Volume] = (
    (__ \ "containerPath").format[String] ~
    (__ \ "hostPath").formatNullable[String] ~
    (__ \ "mode").format[mesos.Volume.Mode] ~
    (__ \ "persistent").formatNullable[PersistentVolumeInfo] ~
    (__ \ "external").formatNullable[ExternalVolumeInfo]
  )(Volume(_, _, _, _, _), unlift(Volume.unapply))

  implicit lazy val ContainerTypeFormat: Format[mesos.ContainerInfo.Type] =
    enumFormat(mesos.ContainerInfo.Type.valueOf, str => s"$str is not a valid container type")

  implicit lazy val ContainerWriter: Writes[Container] = {
    lazy val MesosContainerWrites: Writes[Container.Mesos] = Writes { m =>
      Json.obj(
        "type" -> mesos.ContainerInfo.Type.MESOS,
        "volumes" -> m.volumes,
        "portMappings" -> m.portMappings
      )
    }

    lazy val DockerContainerWrites: Writes[Container.Docker] = Writes { docker =>
      def dockerValues(d: Container.Docker): JsObject = Json.obj(
        "image" -> d.image,
        "privileged" -> d.privileged,
        "parameters" -> d.parameters,
        "forcePullImage" -> d.forcePullImage
      )
      Json.obj(
        "type" -> mesos.ContainerInfo.Type.DOCKER,
        "volumes" -> docker.volumes,
        "portMappings" -> docker.portMappings,
        "docker" -> dockerValues(docker)
      )
    }

    lazy val MesosDockerContainerWrites: Writes[Container.MesosDocker] = Writes { m =>
      def dockerValues(c: Container.MesosDocker): JsObject = Json.obj(
        "image" -> c.image,
        "credential" -> c.credential,
        "forcePullImage" -> c.forcePullImage
      )
      Json.obj(
        "type" -> mesos.ContainerInfo.Type.MESOS,
        "volumes" -> m.volumes,
        "portMappings" -> m.portMappings,
        "docker" -> dockerValues(m)
      )
    }

    lazy val AppCContainerWrites: Writes[Container.MesosAppC] = Writes { appc =>
      def appcValues(a: Container.MesosAppC): JsObject = Json.obj(
        "image" -> a.image,
        "id" -> a.id,
        "labels" -> a.labels,
        "forcePullImage" -> a.forcePullImage
      )
      Json.obj(
        "type" -> mesos.ContainerInfo.Type.MESOS,
        "volumes" -> appc.volumes,
        "portMappings" -> appc.portMappings,
        "appc" -> appcValues(appc)
      )
    }
    Writes {
      case m: Container.Mesos => MesosContainerWrites.writes(m)
      case d: Container.Docker => DockerContainerWrites.writes(d)
      case c: Container.MesosDocker => MesosDockerContainerWrites.writes(c)
      case c: Container.MesosAppC => AppCContainerWrites.writes(c)
    }
  }
}

trait DeploymentFormats {
  import Formats._

  implicit lazy val ByteArrayFormat: Format[Array[Byte]] =
    Format(
      Reads.of[Seq[Int]].map(_.map(_.toByte).toArray),
      Writes { xs =>
        JsArray(xs.to[Seq].map(b => JsNumber(b.toInt)))
      }
    )

  implicit lazy val URLToStringMapFormat: Format[Map[java.net.URL, String]] = Format(
    Reads.of[Map[String, String]]
      .map(
        _.map { case (k, v) => new java.net.URL(k) -> v }
      ),
    Writes[Map[java.net.URL, String]] { m =>
      Json.toJson(m)
    }
  )

  def actionInstanceOn(runSpec: RunSpec): String = runSpec match {
    case _: AppDefinition => "app"
    case _: PodDefinition => "pod"
  }

  implicit lazy val DeploymentActionWrites: Writes[DeploymentAction] = Writes { action =>
    Json.obj(
      "action" -> DeploymentAction.actionName(action),
      actionInstanceOn(action.runSpec) -> action.runSpec.id
    )
  }

  implicit lazy val DeploymentStepWrites: Writes[DeploymentStep] = Json.writes[DeploymentStep]

  implicit lazy val DeploymentStepInfoWrites: Writes[DeploymentStepInfo] = Writes { info =>
    def currentAction(action: DeploymentAction): JsObject = Json.obj (
      "action" -> DeploymentAction.actionName(action),
      actionInstanceOn(action.runSpec) -> action.runSpec.id,
      "readinessCheckResults" -> info.readinessChecksByApp(action.runSpec.id)
    )
    Json.obj(
      "id" -> info.plan.id,
      "version" -> info.plan.version,
      "affectedApps" -> info.plan.affectedAppIds,
      "affectedPods" -> info.plan.affectedPodIds,
      "steps" -> info.plan.steps,
      "currentActions" -> info.step.actions.map(currentAction),
      "currentStep" -> info.nr,
      "totalSteps" -> info.plan.steps.size
    )
  }
}

trait EventFormats {
  import Formats._

  implicit lazy val AppTerminatedEventWrites: Writes[AppTerminatedEvent] = Json.writes[AppTerminatedEvent]

  implicit lazy val PodEventWrites: Writes[PodEvent] = Writes { event =>
    Json.obj(
      "clientIp" -> event.clientIp,
      "uri" -> event.uri,
      "eventType" -> event.eventType,
      "timestamp" -> event.timestamp
    )
  }

  implicit lazy val ApiPostEventWrites: Writes[ApiPostEvent] = Writes { event =>
    Json.obj(
      "clientIp" -> event.clientIp,
      "uri" -> event.uri,
      "appDefinition" -> event.appDefinition,
      "eventType" -> event.eventType,
      "timestamp" -> event.timestamp
    )
  }

  implicit lazy val DeploymentPlanWrites: Writes[DeploymentPlan] = Writes { plan =>
    Json.obj(
      "id" -> plan.id,
      "original" -> plan.original,
      "target" -> plan.target,
      "steps" -> plan.steps,
      "version" -> plan.version
    )
  }

  implicit lazy val SubscribeWrites: Writes[Subscribe] = Json.writes[Subscribe]
  implicit lazy val UnsubscribeWrites: Writes[Unsubscribe] = Json.writes[Unsubscribe]
  implicit lazy val UnhealthyTaskKillEventWrites: Writes[UnhealthyTaskKillEvent] = Json.writes[UnhealthyTaskKillEvent]
  implicit lazy val EventStreamAttachedWrites: Writes[EventStreamAttached] = Json.writes[EventStreamAttached]
  implicit lazy val EventStreamDetachedWrites: Writes[EventStreamDetached] = Json.writes[EventStreamDetached]
  implicit lazy val AddHealthCheckWrites: Writes[AddHealthCheck] = Json.writes[AddHealthCheck]
  implicit lazy val RemoveHealthCheckWrites: Writes[RemoveHealthCheck] = Json.writes[RemoveHealthCheck]
  implicit lazy val FailedHealthCheckWrites: Writes[FailedHealthCheck] = Json.writes[FailedHealthCheck]
  implicit lazy val HealthStatusChangedWrites: Writes[HealthStatusChanged] = Json.writes[HealthStatusChanged]
  implicit lazy val GroupChangeSuccessWrites: Writes[GroupChangeSuccess] = Json.writes[GroupChangeSuccess]
  implicit lazy val GroupChangeFailedWrites: Writes[GroupChangeFailed] = Json.writes[GroupChangeFailed]
  implicit lazy val DeploymentSuccessWrites: Writes[DeploymentSuccess] = Json.writes[DeploymentSuccess]
  implicit lazy val DeploymentFailedWrites: Writes[DeploymentFailed] = Json.writes[DeploymentFailed]
  implicit lazy val DeploymentStatusWrites: Writes[DeploymentStatus] = Json.writes[DeploymentStatus]
  implicit lazy val DeploymentStepSuccessWrites: Writes[DeploymentStepSuccess] = Json.writes[DeploymentStepSuccess]
  implicit lazy val DeploymentStepFailureWrites: Writes[DeploymentStepFailure] = Json.writes[DeploymentStepFailure]
  implicit lazy val MesosStatusUpdateEventWrites: Writes[MesosStatusUpdateEvent] = Json.writes[MesosStatusUpdateEvent]
  implicit lazy val MesosFrameworkMessageEventWrites: Writes[MesosFrameworkMessageEvent] =
    Json.writes[MesosFrameworkMessageEvent]
  implicit lazy val SchedulerDisconnectedEventWrites: Writes[SchedulerDisconnectedEvent] =
    Json.writes[SchedulerDisconnectedEvent]
  implicit lazy val SchedulerRegisteredEventWritesWrites: Writes[SchedulerRegisteredEvent] =
    Json.writes[SchedulerRegisteredEvent]
  implicit lazy val SchedulerReregisteredEventWritesWrites: Writes[SchedulerReregisteredEvent] =
    Json.writes[SchedulerReregisteredEvent]
  implicit lazy val InstanceChangedEventWrites: Writes[InstanceChanged] = Writes { change =>
    Json.obj(
      "instanceId" -> change.id,
      "condition" -> change.condition.toString,
      "runSpecId" -> change.runSpecId,
      "agentId" -> change.instance.agentInfo.agentId,
      "host" -> change.instance.agentInfo.host,
      "runSpecVersion" -> change.runSpecVersion,
      "timestamp" -> change.timestamp,
      "eventType" -> change.eventType
    )
  }
  implicit lazy val InstanceHealthChangedEventWrites: Writes[InstanceHealthChanged] = Writes { change =>
    Json.obj(
      "instanceId" -> change.id,
      "runSpecId" -> change.runSpecId,
      "healthy" -> change.healthy,
      "runSpecVersion" -> change.runSpecVersion,
      "timestamp" -> change.timestamp,
      "eventType" -> change.eventType
    )
  }
  implicit lazy val UnknownInstanceTerminatedEventWrites: Writes[UnknownInstanceTerminated] = Writes { change =>
    Json.obj(
      "instanceId" -> change.id,
      "runSpecId" -> change.runSpecId,
      "condition" -> change.condition.toString,
      "timestamp" -> change.timestamp,
      "eventType" -> change.eventType
    )
  }

  def eventToJson(event: MarathonEvent): JsValue = event match {
    case event: AppTerminatedEvent => Json.toJson(event)
    case event: ApiPostEvent => Json.toJson(event)
    case event: Subscribe => Json.toJson(event)
    case event: Unsubscribe => Json.toJson(event)
    case event: EventStreamAttached => Json.toJson(event)
    case event: EventStreamDetached => Json.toJson(event)
    case event: AddHealthCheck => Json.toJson(event)
    case event: RemoveHealthCheck => Json.toJson(event)
    case event: FailedHealthCheck => Json.toJson(event)
    case event: HealthStatusChanged => Json.toJson(event)
    case event: UnhealthyTaskKillEvent => Json.toJson(event)
    case event: GroupChangeSuccess => Json.toJson(event)
    case event: GroupChangeFailed => Json.toJson(event)
    case event: DeploymentSuccess => Json.toJson(event)
    case event: DeploymentFailed => Json.toJson(event)
    case event: DeploymentStatus => Json.toJson(event)
    case event: DeploymentStepSuccess => Json.toJson(event)
    case event: DeploymentStepFailure => Json.toJson(event)
    case event: MesosStatusUpdateEvent => Json.toJson(event)
    case event: MesosFrameworkMessageEvent => Json.toJson(event)
    case event: SchedulerDisconnectedEvent => Json.toJson(event)
    case event: SchedulerRegisteredEvent => Json.toJson(event)
    case event: SchedulerReregisteredEvent => Json.toJson(event)
    case event: InstanceChanged => Json.toJson(event)
    case event: InstanceHealthChanged => Json.toJson(event)
    case event: UnknownInstanceTerminated => Json.toJson(event)
    case event: PodEvent => Json.toJson(event)
  }
}

trait EventSubscribersFormats {

  implicit lazy val EventSubscribersWrites: Writes[EventSubscribers] = Writes { eventSubscribers =>
    Json.obj(
      "callbackUrls" -> eventSubscribers.urls
    )
  }
}

trait HealthCheckFormats {
  import Formats._

  /*
   * HealthCheck related formats
   */

  implicit lazy val HealthWrites: Writes[Health] = Writes { health =>
    Json.obj(
      "alive" -> health.alive,
      "consecutiveFailures" -> health.consecutiveFailures,
      "firstSuccess" -> health.firstSuccess,
      "lastFailure" -> health.lastFailure,
      "lastSuccess" -> health.lastSuccess,
      "lastFailureCause" -> health.lastFailureCause.fold[JsValue](JsNull)(JsString),
      "taskId" -> health.taskId
    )
  }

  implicit lazy val HealthCheckProtocolFormat: Format[Protocol] =
    enumFormat(Protocol.valueOf, str => s"$str is not a valid protocol")

  val BasicHealthCheckFormatBuilder = {
    import mesosphere.marathon.core.health.HealthCheck._

    (__ \ "gracePeriodSeconds").formatNullable[Long].withDefault(DefaultGracePeriod.toSeconds).asSeconds ~
      (__ \ "intervalSeconds").formatNullable[Long].withDefault(DefaultInterval.toSeconds).asSeconds ~
      (__ \ "timeoutSeconds").formatNullable[Long].withDefault(DefaultTimeout.toSeconds).asSeconds ~
      (__ \ "maxConsecutiveFailures").formatNullable[Int].withDefault(DefaultMaxConsecutiveFailures)
  }

  implicit lazy val PortReferenceFormat: Format[PortReference] = Format[PortReference](
    Reads[PortReference] { js =>
      js.asOpt[Int].map { intIndex =>
        JsSuccess(PortReference(intIndex))
      }.getOrElse {
        js.asOpt[String].map { stringIndex =>
          JsSuccess(PortReference(stringIndex))
        }.getOrElse {
          JsError("expected string (port name) or integer (port offset) for port-index")
        }
      }
    },
    Writes[PortReference] {
      case byInt: PortReference.ByIndex => JsNumber(byInt.value)
      case byName: PortReference.ByName => JsString(byName.value)
    }
  )

  val HealthCheckWithPortsFormatBuilder =
    BasicHealthCheckFormatBuilder ~
      (__ \ "portIndex").formatNullable[PortReference] ~
      (__ \ "port").formatNullable[Int]

  val HttpHealthCheckFormatBuilder = {
    import mesosphere.marathon.core.health.MarathonHttpHealthCheck._

    HealthCheckWithPortsFormatBuilder ~
      (__ \ "path").formatNullable[String] ~
      (__ \ "protocol").formatNullable[Protocol].withDefault(DefaultProtocol)
  }

  // Marathon health checks formats
  implicit val MarathonHttpHealthCheckFormat: Format[MarathonHttpHealthCheck] = {
    (
      HttpHealthCheckFormatBuilder ~
      (__ \ "ignoreHttp1xx").formatNullable[Boolean].withDefault(MarathonHttpHealthCheck.DefaultIgnoreHttp1xx)
    )(MarathonHttpHealthCheck.apply, unlift(MarathonHttpHealthCheck.unapply))
  }

  implicit val MarathonTcpHealthCheckFormat: Format[MarathonTcpHealthCheck] =
    HealthCheckWithPortsFormatBuilder(MarathonTcpHealthCheck.apply, unlift(MarathonTcpHealthCheck.unapply))

  // Mesos health checks formats
  implicit val MesosHttpHealthCheckFormat: Format[MesosHttpHealthCheck] = {
    (
      HttpHealthCheckFormatBuilder ~
      (__ \ "delaySeconds").formatNullable[Long].withDefault(HealthCheck.DefaultDelay.toSeconds).asSeconds
    )(MesosHttpHealthCheck.apply, unlift(MesosHttpHealthCheck.unapply))
  }

  implicit val ExecutableFormat: Format[Executable] = Format[Executable] (
    Reads[Executable] { js => js.validate[Command].flatMap(cmd => JsSuccess[Executable](cmd)) },
    Writes[Executable] {
      case c: Command => CommandFormat.writes(c)
      case e: ArgvList => throw SerializationFailedException("serialization of ArgvList not supported")
    }
  )

  implicit val MesosCommandHealthCheckFormat: Format[MesosCommandHealthCheck] = (
    BasicHealthCheckFormatBuilder ~
    (__ \ "delaySeconds").formatNullable[Long].withDefault(HealthCheck.DefaultDelay.toSeconds).asSeconds ~
    (__ \ "command").format[Executable]
  )(MesosCommandHealthCheck.apply, unlift(MesosCommandHealthCheck.unapply))

  implicit val MesosTcpHealthCheckFormat: Format[MesosTcpHealthCheck] = {
    (
      HealthCheckWithPortsFormatBuilder ~
      (__ \ "delaySeconds").formatNullable[Long].withDefault(HealthCheck.DefaultDelay.toSeconds).asSeconds
    )(MesosTcpHealthCheck.apply, unlift(MesosTcpHealthCheck.unapply))
  }

  implicit val HealthCheckFormat: Format[HealthCheck] = Format[HealthCheck] (
    new Reads[HealthCheck] {
      override def reads(json: JsValue): JsResult[HealthCheck] = {
        val result = (json \ "protocol").validateOpt[Protocol](HealthCheckProtocolFormat)

        result.flatMap {
          _.getOrElse(HealthCheck.DefaultProtocol) match {
            case Protocol.COMMAND => json.validate[MesosCommandHealthCheck]
            case Protocol.HTTP | Protocol.HTTPS => json.validate[MarathonHttpHealthCheck]
            case Protocol.TCP => json.validate[MarathonTcpHealthCheck]
            case Protocol.MESOS_HTTP | Protocol.MESOS_HTTPS => json.validate[MesosHttpHealthCheck]
            case Protocol.MESOS_TCP => json.validate[MesosTcpHealthCheck]
          }
        }
      }
    },
    Writes[HealthCheck] {
      case tcp: MarathonTcpHealthCheck =>
        Json.toJson(tcp)(MarathonTcpHealthCheckFormat).as[JsObject] ++ Json.obj("protocol" -> "TCP")
      case http: MarathonHttpHealthCheck =>
        Json.toJson(http)(MarathonHttpHealthCheckFormat).as[JsObject]
      case command: MesosCommandHealthCheck =>
        Json.toJson(command)(MesosCommandHealthCheckFormat).as[JsObject] ++ Json.obj("protocol" -> "COMMAND")
      case tcp: MesosTcpHealthCheck =>
        Json.toJson(tcp)(MesosTcpHealthCheckFormat).as[JsObject] ++ Json.obj("protocol" -> "MESOS_TCP")
      case http: MesosHttpHealthCheck =>
        Json.toJson(http)(MesosHttpHealthCheckFormat).as[JsObject]
    }
  )
}

trait ReadinessCheckFormats {
  import Formats._
  import mesosphere.marathon.core.readiness._

  implicit lazy val ReadinessCheckFormat: Format[ReadinessCheck] = {
    import ReadinessCheck._

    (
      (__ \ "name").formatNullable[String].withDefault(DefaultName) ~
      (__ \ "protocol").formatNullable[ReadinessCheck.Protocol].withDefault(DefaultProtocol) ~
      (__ \ "path").formatNullable[String].withDefault(DefaultPath) ~
      (__ \ "portName").formatNullable[String].withDefault(DefaultPortName) ~
      (__ \ "intervalSeconds").formatNullable[Long].withDefault(DefaultInterval.toSeconds).asSeconds ~
      (__ \ "timeoutSeconds").formatNullable[Long].withDefault(DefaultTimeout.toSeconds).asSeconds ~
      (__ \ "httpStatusCodesForReady").formatNullable[Set[Int]].withDefault(DefaultHttpStatusCodesForReady) ~
      (__ \ "preserveLastResponse").formatNullable[Boolean].withDefault(DefaultPreserveLastResponse)
    )(ReadinessCheck.apply, unlift(ReadinessCheck.unapply))
  }

  implicit lazy val ReadinessCheckProtocolFormat: Format[ReadinessCheck.Protocol] = {
    Format(
      Reads[ReadinessCheck.Protocol] {
        case JsString(string) =>
          StringToProtocol.get(string) match {
            case Some(protocol) => JsSuccess(protocol)
            case None => JsError(ProtocolErrorString)
          }
        case _: JsValue => JsError(ProtocolErrorString)
      },
      Writes[ReadinessCheck.Protocol](protocol => JsString(ProtocolToString(protocol)))
    )
  }
  implicit lazy val ReadinessCheckResultFormat: Format[ReadinessCheckResult] = Json.format[ReadinessCheckResult]
  implicit lazy val ReadinessCheckHttpResponseFormat: Format[HttpResponse] = Json.format[HttpResponse]

  private[this] val ProtocolToString = Map[ReadinessCheck.Protocol, String](
    ReadinessCheck.Protocol.HTTP -> "HTTP",
    ReadinessCheck.Protocol.HTTPS -> "HTTPS"
  )
  private[this] val StringToProtocol: Map[String, ReadinessCheck.Protocol] =
    ProtocolToString.map { case (k, v) => (v, k) }
  private[this] val ProtocolErrorString = s"Choose one of ${StringToProtocol.keys.mkString(", ")}"
}

trait FetchUriFormats {
  import Formats._

  implicit lazy val FetchUriFormat: Format[FetchUri] = {
    (
      (__ \ "uri").format[String] ~
      (__ \ "extract").formatNullable[Boolean].withDefault(true) ~
      (__ \ "executable").formatNullable[Boolean].withDefault(false) ~
      (__ \ "cache").formatNullable[Boolean].withDefault(false) ~
      (__ \ "outputFile").formatNullable[String]
    )(FetchUri(_, _, _, _, _), unlift(FetchUri.unapply))
  }
}

trait SecretFormats {
  implicit lazy val SecretFormat = Json.format[Secret]
}

@SuppressWarnings(Array("PartialFunctionInsteadOfMatch"))
trait AppAndGroupFormats {

  import Formats._

  implicit lazy val IdentifiableWrites = Json.writes[Identifiable]

  implicit lazy val UpgradeStrategyWrites = Json.writes[UpgradeStrategy]
  implicit lazy val UpgradeStrategyReads: Reads[UpgradeStrategy] = {
    import mesosphere.marathon.state.AppDefinition._
    (
      (__ \ "minimumHealthCapacity").readNullable[Double].withDefault(DefaultUpgradeStrategy.minimumHealthCapacity) ~
      (__ \ "maximumOverCapacity").readNullable[Double].withDefault(DefaultUpgradeStrategy.maximumOverCapacity)
    ) (UpgradeStrategy(_, _))
  }

  implicit lazy val ConstraintFormat: Format[Constraint] = Format(
    new Reads[Constraint] {
      @SuppressWarnings(Array("TraversableHead"))
      override def reads(json: JsValue): JsResult[Constraint] = {
        val validOperators = Operator.values().map(_.toString)

        json.asOpt[Seq[String]] match {
          case Some(seq) if seq.size >= 2 && seq.size <= 3 =>
            if (validOperators.contains(seq(1))) {
              val builder = Constraint.newBuilder().setField(seq.head).setOperator(Operator.valueOf(seq(1)))
              if (seq.size == 3) builder.setValue(seq(2))
              JsSuccess(builder.build())
            } else {
              JsError(s"Constraint operator must be one of the following: [${validOperators.mkString(", ")}]")
            }
          case _ => JsError("Constraint definition must be an array of strings in format: <key>, <operator>[, value]")
        }
      }
    },
    Writes[Constraint] { constraint =>
      val builder = Seq.newBuilder[JsString]
      builder += JsString(constraint.getField)
      builder += JsString(constraint.getOperator.name)
      if (constraint.hasValue) builder += JsString(constraint.getValue)
      JsArray(builder.result())
    }
  )

  implicit lazy val EnvVarSecretRefFormat: Format[EnvVarSecretRef] = Json.format[EnvVarSecretRef]
  implicit lazy val EnvVarValueFormat: Format[EnvVarValue] = Format(
    new Reads[EnvVarValue] {
      override def reads(json: JsValue): JsResult[EnvVarValue] = {
        json.asOpt[String] match {
          case Some(stringValue) => JsSuccess(EnvVarString(stringValue))
          case _ => JsSuccess(json.as[EnvVarSecretRef])
        }
      }
    },
    new Writes[EnvVarValue] {
      override def writes(envvar: EnvVarValue): JsValue = {
        envvar match {
          case s: EnvVarString => JsString(s.value)
          case ref: EnvVarSecretRef => EnvVarSecretRefFormat.writes(ref)
        }
      }
    }
  )

  implicit lazy val taskLostBehaviorWrites = Writes[TaskLostBehavior] { taskLostBehavior =>
    JsString(taskLostBehavior.name())
  }

  implicit lazy val taskLostBehaviorReads = Reads[TaskLostBehavior] { json =>
    json.validate[String].flatMap { behaviorString: String =>

      Option(TaskLostBehavior.valueOf(behaviorString)) match {
        case Some(taskLostBehavior) => JsSuccess(taskLostBehavior)
        case None =>
          val allowedTaskLostBehaviorString =
            TaskLostBehavior.values().toSeq.map(_.getDescriptorForType.getName).mkString(", ")

          JsError(s"'$behaviorString' is not a valid taskLostBehavior. Allowed values: $allowedTaskLostBehaviorString")
      }

    }
  }

  implicit lazy val ResidencyFormat: Format[Residency] = (
    (__ \ "relaunchEscalationTimeoutSeconds").formatNullable[Long]
    .withDefault(Residency.defaultRelaunchEscalationTimeoutSeconds) ~
    (__ \ "taskLostBehavior").formatNullable[TaskLostBehavior]
    .withDefault(Residency.defaultTaskLostBehaviour)
  ) (Residency(_, _), unlift(Residency.unapply))

  implicit lazy val RunSpecWrites: Writes[RunSpec] = {
    Writes[RunSpec] {
      case app: AppDefinition => Json.toJson(Raml.toRaml(app))
      case pod: PodDefinition => Json.toJson(Raml.toRaml(pod))
    }
  }

  implicit lazy val VersionInfoWrites: Writes[VersionInfo] =
    Writes[VersionInfo] {
      case VersionInfo.FullVersionInfo(_, lastScalingAt, lastConfigChangeAt) =>
        Json.obj(
          "lastScalingAt" -> lastScalingAt,
          "lastConfigChangeAt" -> lastConfigChangeAt
        )

      case VersionInfo.OnlyVersion(version) => JsNull
      case VersionInfo.NoVersion => JsNull
    }

  implicit lazy val TaskCountsWrites: Writes[TaskCounts] =
    Writes { counts =>
      Json.obj(
        "tasksStaged" -> counts.tasksStaged,
        "tasksRunning" -> counts.tasksRunning,
        "tasksHealthy" -> counts.tasksHealthy,
        "tasksUnhealthy" -> counts.tasksUnhealthy
      )
    }

  lazy val TaskCountsWritesWithoutPrefix: Writes[TaskCounts] =
    Writes { counts =>
      Json.obj(
        "staged" -> counts.tasksStaged,
        "running" -> counts.tasksRunning,
        "healthy" -> counts.tasksHealthy,
        "unhealthy" -> counts.tasksUnhealthy
      )
    }

  implicit lazy val TaskLifeTimeWrites: Writes[TaskLifeTime] =
    Writes { lifeTime =>
      Json.obj(
        "averageSeconds" -> lifeTime.averageSeconds,
        "medianSeconds" -> lifeTime.medianSeconds
      )
    }

  implicit lazy val TaskStatsWrites: Writes[TaskStats] =
    Writes { stats =>
      val statsJson = Json.obj("counts" -> TaskCountsWritesWithoutPrefix.writes(stats.counts))
      Json.obj(
        "stats" -> stats.maybeLifeTime.fold(ifEmpty = statsJson)(lifeTime =>
          statsJson ++ Json.obj("lifeTime" -> lifeTime)
        )
      )
    }

  @SuppressWarnings(Array("PartialFunctionInsteadOfMatch"))
  implicit lazy val TaskStatsByVersionWrites: Writes[TaskStatsByVersion] =
    Writes { byVersion =>
      val maybeJsons = Map[String, Option[TaskStats]](
        "startedAfterLastScaling" -> byVersion.maybeStartedAfterLastScaling,
        "withLatestConfig" -> byVersion.maybeWithLatestConfig,
        "withOutdatedConfig" -> byVersion.maybeWithOutdatedConfig,
        "totalSummary" -> byVersion.maybeTotalSummary
      )
      Json.toJson(
        maybeJsons.flatMap {
          case (k, v) => v.map(k -> TaskStatsWrites.writes(_))
        }
      )
    }

  implicit lazy val ExtendedAppInfoWrites: Writes[AppInfo] =
    Writes { info =>
      val appJson = RunSpecWrites.writes(info.app).as[JsObject]

      val maybeJson = Seq[Option[JsObject]](
        info.maybeCounts.map(TaskCountsWrites.writes(_).as[JsObject]),
        info.maybeDeployments.map(deployments => Json.obj("deployments" -> deployments)),
        info.maybeReadinessCheckResults.map(readiness => Json.obj("readinessCheckResults" -> readiness)),
        info.maybeTasks.map(tasks => Json.obj("tasks" -> tasks)),
        info.maybeLastTaskFailure.map(lastFailure => Json.obj("lastTaskFailure" -> lastFailure)),
        info.maybeTaskStats.map(taskStats => Json.obj("taskStats" -> taskStats))
      ).flatten

      maybeJson.foldLeft(appJson)((result, obj) => result ++ obj)
    }

  implicit lazy val GroupInfoWrites: Writes[GroupInfo] =
    Writes { info =>

      val maybeJson = Seq[Option[JsObject]](
        info.maybeApps.map(apps => Json.obj("apps" -> apps)),
        info.maybeGroups.map(groups => Json.obj("groups" -> groups)),
        info.maybePods.map(pods => Json.obj("pods" -> pods))
      ).flatten

      val groupJson = Json.obj (
        "id" -> info.group.id,
        "dependencies" -> info.group.dependencies,
        "version" -> info.group.version
      )

      maybeJson.foldLeft(groupJson)((result, obj) => result ++ obj)
    }

  implicit lazy val GroupFormatWrites: Writes[Group] = (
    (__ \ "id").write[PathId] ~
    (__ \ "apps").write[Iterable[App]] ~
    (__ \ "pods").write[Iterable[Pod]] ~
    (__ \ "groups").lazyWrite(implicitly[Writes[Iterable[Group]]]) ~
    (__ \ "dependencies").write[Set[PathId]] ~
    (__ \ "version").write[Timestamp]
  ) ({ (g: Group) =>
      (
        g.id,
        g.apps.values.map(Raml.toRaml(_)),
        g.pods.values.map(Raml.toRaml(_)),
        g.groupsById.values,
        g.dependencies,
        g.version
      )
    })

  implicit lazy val PortDefinitionFormat: Format[PortDefinition] = (
    (__ \ "port").formatNullable[Int].withDefault(AppDefinition.RandomPortValue) ~
    (__ \ "protocol").formatNullable[String].withDefault("tcp") ~
    (__ \ "name").formatNullable[String] ~
    (__ \ "labels").formatNullable[Map[String, String]].withDefault(Map.empty[String, String])
  )(PortDefinition(_, _, _, _), unlift(PortDefinition.unapply))
}

trait PluginFormats {

  implicit lazy val pluginDefinitionFormat: Writes[PluginDefinition] = (
    (__ \ "id").write[String] ~
    (__ \ "plugin").write[String] ~
    (__ \ "implementation").write[String] ~
    (__ \ "tags").writeNullable[Set[String]] ~
    (__ \ "info").writeNullable[JsObject]
  ) (d => (d.id, d.plugin, d.implementation, d.tags, d.info))

  implicit lazy val pluginDefinitionsFormat: Writes[PluginDefinitions] = Json.writes[PluginDefinitions]
}
