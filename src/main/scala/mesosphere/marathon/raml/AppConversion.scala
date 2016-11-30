package mesosphere.marathon
package raml

import mesosphere.marathon.Protos.ResidencyDefinition
import mesosphere.marathon.state._

import scala.concurrent.duration._

trait AppConversion extends ConstraintConversion with EnvVarConversion with HealthCheckConversion
    with NetworkConversion with ReadinessConversions with SecretConversion with VolumeConversion with UnreachableStrategyConversion {

  import AppConversion._

  implicit val pathIdWrites: Writes[PathId, String] = Writes { _.toString }

  implicit val artifactWrites: Writes[FetchUri, Artifact] = Writes { fetch =>
    Artifact(fetch.uri, Some(fetch.extract), Some(fetch.executable), Some(fetch.cache))
  }

  implicit val upgradeStrategyWrites: Writes[state.UpgradeStrategy, UpgradeStrategy] = Writes { strategy =>
    UpgradeStrategy(strategy.maximumOverCapacity, strategy.minimumHealthCapacity)
  }

  implicit val appResidencyWrites: Writes[Residency, AppResidency] = Writes { residency =>
    AppResidency(residency.relaunchEscalationTimeoutSeconds.toInt, residency.taskLostBehavior.toRaml)
  }

  implicit val versionInfoWrites: Writes[state.VersionInfo, Option[VersionInfo]] = Writes {
    case state.VersionInfo.FullVersionInfo(_, scale, config) => Some(VersionInfo(scale.toOffsetDateTime, config.toOffsetDateTime))
    case state.VersionInfo.OnlyVersion(version) => None
    case state.VersionInfo.NoVersion => None
  }

  implicit val parameterWrites: Writes[state.Parameter, DockerParameter] = Writes { param =>
    DockerParameter(param.key, param.value)
  }

  implicit val appWriter: Writes[AppDefinition, App] = Writes { app =>
    // we explicitly do not write ports, uris, ipAddress because they are deprecated fields
    App(
      id = app.id.toString,
      acceptedResourceRoles = if (app.acceptedResourceRoles.nonEmpty) Some(app.acceptedResourceRoles) else None,
      args = app.args,
      backoffFactor = app.backoffStrategy.factor,
      backoffSeconds = app.backoffStrategy.backoff.toSeconds.toInt,
      cmd = app.cmd,
      constraints = app.constraints.toRaml[Seq[Seq[String]]],
      container = app.container.toRaml,
      cpus = app.resources.cpus,
      dependencies = app.dependencies.map(Raml.toRaml(_)),
      disk = app.resources.disk,
      env = app.env.toRaml,
      executor = app.executor,
      fetch = app.fetch.toRaml,
      gpus = app.resources.gpus,
      healthChecks = app.healthChecks.toRaml,
      instances = app.instances,
      labels = app.labels,
      maxLaunchDelaySeconds = app.backoffStrategy.maxLaunchDelay.toSeconds.toInt,
      mem = app.resources.mem,
      networks = app.networks.toRaml,
      portDefinitions = if (app.portDefinitions.isEmpty) None else Some(app.portDefinitions.toRaml),
      readinessChecks = app.readinessChecks.toRaml,
      residency = app.residency.toRaml,
      requirePorts = Some(app.requirePorts),
      secrets = app.secrets.toRaml,
      storeUrls = app.storeUrls,
      taskKillGracePeriodSeconds = app.taskKillGracePeriod.map(_.toSeconds.toInt),
      upgradeStrategy = Some(app.upgradeStrategy.toRaml),
      user = app.user,
      version = Some(app.versionInfo.version.toOffsetDateTime),
      versionInfo = Some(app.versionInfo.toRaml),
      unreachableStrategy = Some(app.unreachableStrategy.toRaml)
    )
  }

  def resources(cpus: Option[Double], mem: Option[Double], disk: Option[Double], gpus: Option[Int]): Resources =
    Resources(
      cpus = cpus.getOrElse(AppDefinition.DefaultCpus),
      mem = mem.getOrElse(AppDefinition.DefaultMem),
      disk = disk.getOrElse(AppDefinition.DefaultDisk),
      gpus = gpus.getOrElse(AppDefinition.DefaultGpus)
    )

  implicit val residencyRamlReader = Reads[AppResidency, Residency] { residency =>
    import ResidencyDefinition.TaskLostBehavior._
    Residency(
      relaunchEscalationTimeoutSeconds = residency.relaunchEscalationTimeoutSeconds.toLong,
      taskLostBehavior = residency.taskLostBehavior match {
        case TaskLostBehavior.RelaunchAfterTimeout => RELAUNCH_AFTER_TIMEOUT
        case TaskLostBehavior.WaitForever => WAIT_FOREVER
      }
    )
  }

  implicit val fetchUriReader = Reads[Artifact, FetchUri] { artifact =>
    import FetchUri.defaultInstance
    FetchUri(
      uri = artifact.uri,
      extract = artifact.extract.getOrElse(defaultInstance.extract),
      executable = artifact.executable.getOrElse(defaultInstance.executable),
      cache = artifact.cache.getOrElse(defaultInstance.cache),
      outputFile = artifact.destPath.orElse(defaultInstance.outputFile)
    )
  }

  implicit val portDefinitionRamlReader = Reads[PortDefinition, state.PortDefinition] { portDef =>
    val protocol: String = portDef.protocol match {
      case NetworkProtocol.Tcp => "tcp"
      case NetworkProtocol.Udp => "udp"
      case NetworkProtocol.UdpTcp => "udp,tcp"
    }

    state.PortDefinition(
      port = portDef.port,
      protocol = protocol,
      name = portDef.name,
      labels = portDef.labels
    )
  }

  implicit val portMappingRamlReader = Reads[ContainerPortMapping, state.Container.PortMapping] {
    case ContainerPortMapping(containerPort, hostPort, labels, name, protocol, servicePort) =>
      import state.Container.PortMapping._
      val decodedProto = protocol match {
        case NetworkProtocol.Tcp => TCP
        case NetworkProtocol.Udp => UDP
        case NetworkProtocol.UdpTcp => UDP_TCP
      }
      state.Container.PortMapping(
        containerPort = containerPort,
        hostPort = hostPort.orElse(defaultInstance.hostPort),
        servicePort = servicePort,
        protocol = decodedProto,
        name = name,
        labels = labels
      )
  }

  implicit val appContainerRamlReader = Reads[Container, state.Container] { (container: Container) =>
    val volumes = container.volumes.map(Raml.fromRaml(_))
    val portMappings = container.portMappings.map(Raml.fromRaml(_))

    val result: state.Container = (container.`type`, container.docker, container.appc) match {
      case (EngineType.Docker, Some(docker), None) =>
        state.Container.Docker(
          volumes = volumes,
          image = docker.image,
          portMappings = portMappings, // assumed already normalized, see Formats
          privileged = docker.privileged.getOrElse(false),
          parameters = docker.parameters.map(p => Parameter(p.key, p.value)),
          forcePullImage = docker.forcePullImage.getOrElse(false)
        )
      case (EngineType.Mesos, Some(docker), None) =>
        state.Container.MesosDocker(
          volumes = volumes,
          image = docker.image,
          portMappings = portMappings, // assumed already normalized, see Formats
          credential = docker.credential.map(c => state.Container.Credential(principal = c.principal, secret = c.secret)),
          forcePullImage = docker.forcePullImage.getOrElse(false)
        )
      case (EngineType.Mesos, None, Some(appc)) =>
        state.Container.MesosAppC(
          volumes = volumes,
          image = appc.image,
          portMappings = portMappings,
          id = appc.id,
          labels = appc.labels,
          forcePullImage = appc.forcePullImage.getOrElse(false)
        )
      case (EngineType.Mesos, None, None) =>
        state.Container.Mesos(
          volumes = volumes,
          portMappings = portMappings
        )
      case ct => throw SerializationFailedException(s"illegal container specification $ct")
    }
    result
  }

  implicit val upgradeStrategyRamlReader = Reads[UpgradeStrategy, state.UpgradeStrategy] { us =>
    state.UpgradeStrategy(
      maximumOverCapacity = us.maximumOverCapacity,
      minimumHealthCapacity = us.minimumHealthCapacity
    )
  }

  implicit val appRamlReader: Reads[App, AppDefinition] = Reads[App, AppDefinition] { app =>
    // TODO not all validation has been applied to the raml; most app validation still just validates the model.
    // there's also some code here that would probably be better off in a raml.App `normalization` func.

    val selectedStrategy = ResidencyAndUpgradeStrategy(
      app.residency.map(Raml.fromRaml(_)),
      app.upgradeStrategy.map(Raml.fromRaml(_)),
      app.container.exists(_.volumes.exists(_.persistent.nonEmpty)),
      app.container.exists(_.volumes.exists(_.external.nonEmpty))
    )

    val backoffStrategy = BackoffStrategy(
      backoff = app.backoffSeconds.seconds,
      maxLaunchDelay = app.maxLaunchDelaySeconds.seconds,
      factor = app.backoffFactor
    )

    val versionInfo = state.VersionInfo.OnlyVersion(app.version.map(Timestamp(_)).getOrElse(Timestamp.now()))

    val result: AppDefinition = AppDefinition(
      id = PathId(app.id),
      cmd = app.cmd,
      args = app.args,
      user = app.user,
      env = Raml.fromRaml(app.env),
      instances = app.instances,
      resources = resources(Some(app.cpus), Some(app.mem), Some(app.disk), Some(app.gpus)),
      executor = app.executor,
      constraints = app.constraints.map(Raml.fromRaml(_))(collection.breakOut),
      fetch = app.fetch.map(Raml.fromRaml(_)),
      storeUrls = app.storeUrls,
      portDefinitions = app.portDefinitions.map(_.map(Raml.fromRaml(_))).getOrElse(Nil),
      requirePorts = app.requirePorts.getOrElse(AppDefinition.DefaultRequirePorts),
      backoffStrategy = backoffStrategy,
      container = app.container.map(Raml.fromRaml(_)),
      healthChecks = app.healthChecks.map(Raml.fromRaml(_)).toSet,
      readinessChecks = app.readinessChecks.map(Raml.fromRaml(_)),
      taskKillGracePeriod = app.taskKillGracePeriodSeconds.map(_.second),
      dependencies = app.dependencies.map(PathId(_))(collection.breakOut),
      upgradeStrategy = selectedStrategy.upgradeStrategy,
      labels = app.labels,
      acceptedResourceRoles = app.acceptedResourceRoles.getOrElse(AppDefinition.DefaultAcceptedResourceRoles),
      networks = app.networks.map(Raml.fromRaml(_)),
      versionInfo = versionInfo,
      residency = selectedStrategy.residency,
      secrets = Raml.fromRaml(app.secrets),
      unreachableStrategy = app.unreachableStrategy.map(_.fromRaml).getOrElse(AppDefinition.DefaultUnreachableStrategy)
    )
    result
  }

  implicit val appUpdateRamlReader: Reads[(AppUpdate, AppDefinition), AppDefinition] = Reads { src =>
    val (update: AppUpdate, app: AppDefinition) = src
    app.copy(
      // id stays the same
      cmd = update.cmd.orElse(app.cmd),
      args = update.args.getOrElse(app.args),
      user = update.user.orElse(app.user),
      env = update.env.fold(app.env)(Raml.fromRaml(_)),
      instances = update.instances.getOrElse(app.instances),
      resources = Resources(
        cpus = update.cpus.getOrElse(app.resources.cpus),
        mem = update.mem.getOrElse(app.resources.mem),
        disk = update.disk.getOrElse(app.resources.disk),
        gpus = update.gpus.getOrElse(app.resources.gpus)
      ),
      executor = update.executor.getOrElse(app.executor),
      constraints = update.constraints.fold(app.constraints)(c => c.map(Raml.fromRaml(_))(collection.breakOut)),
      fetch = update.fetch.fold(app.fetch)(f => f.map(Raml.fromRaml(_))),
      storeUrls = update.storeUrls.getOrElse(app.storeUrls),
      portDefinitions = update.portDefinitions.fold(app.portDefinitions)(p => p.map(Raml.fromRaml(_))),
      requirePorts = update.requirePorts.getOrElse(app.requirePorts),
      backoffStrategy = BackoffStrategy(
        backoff = update.backoffSeconds.fold(app.backoffStrategy.backoff)(_.seconds),
        factor = update.backoffFactor.getOrElse(app.backoffStrategy.factor),
        maxLaunchDelay = update.maxLaunchDelaySeconds.fold(app.backoffStrategy.maxLaunchDelay)(_.seconds)
      ),
      container = update.container.map(Raml.fromRaml(_)).orElse(app.container),
      healthChecks = update.healthChecks.fold(app.healthChecks)(h => h.map(Raml.fromRaml(_)).toSet),
      readinessChecks = update.readinessChecks.fold(app.readinessChecks)(r => r.map(Raml.fromRaml(_))),
      dependencies = update.dependencies.fold(app.dependencies)(deps => deps.map(PathId(_).canonicalPath(app.id))(collection.breakOut)),
      upgradeStrategy = update.upgradeStrategy.fold(app.upgradeStrategy)(Raml.fromRaml(_)),
      labels = update.labels.getOrElse(app.labels),
      acceptedResourceRoles = update.acceptedResourceRoles.getOrElse(app.acceptedResourceRoles),
      networks = update.networks.fold(app.networks)(nets => nets.map(Raml.fromRaml(_))),
      // versionInfo doesn't change - it's never overridden by an AppUpdate
      // Setting the version in AppUpdate means that the user wants to revert to that version. In that
      // case, we do not update the current AppDefinition but revert completely to the specified version.
      // For all other updates, the GroupVersioningUtil will determine a new version if the AppDefinition
      // has really changed.
      residency = update.residency.map(Raml.fromRaml(_)).orElse(app.residency),
      secrets = update.secrets.fold(app.secrets)(Raml.fromRaml(_)),
      taskKillGracePeriod = update.taskKillGracePeriodSeconds.map(_.seconds).orElse(app.taskKillGracePeriod),
      unreachableStrategy = update.unreachableStrategy.map(_.fromRaml).orElse(app.unreachableStrategy)
    )
  }
}

object AppConversion extends AppConversion {

  case class ResidencyAndUpgradeStrategy(residency: Option[Residency], upgradeStrategy: state.UpgradeStrategy)

  object ResidencyAndUpgradeStrategy {
    def apply(
      residency: Option[Residency],
      upgradeStrategy: Option[state.UpgradeStrategy],
      hasPersistentVolumes: Boolean,
      hasExternalVolumes: Boolean): ResidencyAndUpgradeStrategy = {

      import state.UpgradeStrategy.{ empty, forResidentTasks }

      val residencyOrDefault: Option[Residency] =
        residency.orElse(if (hasPersistentVolumes) Some(Residency.defaultResidency) else None)

      val selectedUpgradeStrategy = upgradeStrategy.getOrElse {
        if (residencyOrDefault.isDefined || hasExternalVolumes) forResidentTasks else empty
      }

      ResidencyAndUpgradeStrategy(residencyOrDefault, selectedUpgradeStrategy)
    }
  }
}
