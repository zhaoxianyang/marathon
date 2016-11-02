package mesosphere.marathon
package api.v2.validation

import java.util.regex.Pattern

import com.wix.accord._
import com.wix.accord.dsl._
import mesosphere.marathon.api.v2.Validation._
import mesosphere.marathon.core.externalvolume.ExternalVolumes
import mesosphere.marathon.raml._
import mesosphere.marathon.state.{ AppDefinition, PathId, PortAssignment, ResourceRole }

import scala.util.Try

trait AppValidation {
  import AppValidation._
  import ArtifactValidation._
  import EnvVarValidation._
  import NetworkValidation._
  import SecretValidation._
  import SchedulingValidator._

  val validPortNumber = validator[Int] { port =>
    port should be >= 0 and be <= 65535
  }

  implicit val appResidencyValidator: Validator[AppResidency] = validator[AppResidency] { residency =>
    residency.relaunchEscalationTimeoutSeconds >= 0
  }

  implicit val portDefinitionValidator = validator[PortDefinition] { portDefinition =>
    portDefinition.port is validPortNumber
    portDefinition.name is optional(matchRegexFully(PortAssignment.PortNamePattern))
  }

  val portDefinitionsValidator: Validator[Seq[PortDefinition]] = validator[Seq[PortDefinition]] {
    portDefinitions =>
      portDefinitions is every(valid)
      portDefinitions is elementsAreUniqueByOptional(_.name, "Port names must be unique.")
      portDefinitions is elementsAreUniqueBy(_.port, "Ports must be unique.",
        filter = { (port: Int) => port != AppDefinition.RandomPortValue })
  }

  implicit val portMappingValidator = validator[ContainerPortMapping] { portMapping =>
    portMapping.containerPort is validPortNumber
    portMapping.hostPort should optional(validPortNumber)
    portMapping.servicePort should valid(validPortNumber)
    portMapping.name is optional(matchRegexFully(PortAssignment.PortNamePattern))
  }

  val portMappingsValidator = validator[Seq[ContainerPortMapping]] { portMappings =>
    portMappings is every(valid)
    portMappings is elementsAreUniqueByOptional(_.name, "Port names must be unique.")
  }

  val dockerDockerContainerValidator: Validator[Container] = {
    val validDockerEngineSpec: Validator[DockerContainer] = validator[DockerContainer] { docker =>
      docker.image is notEmpty
      docker.portMappings is valid(portMappingsValidator)
    }
    validator { (container: Container) =>
      container.docker is valid(definedAnd(validDockerEngineSpec))
    }
  }

  val mesosDockerContainerValidator: Validator[Container] = {
    val validMesosEngineSpec: Validator[DockerContainer] = validator[DockerContainer] { docker =>
      docker.image is notEmpty
    }
    validator{ (container: Container) =>
      container.docker is valid(definedAnd(validMesosEngineSpec))
    }
  }

  val mesosAppcContainerValidator: Validator[Container] = {
    val prefix = "sha512-"

    val validId: Validator[String] =
      isTrue[String](s"id must begin with '$prefix',") { id =>
        id.startsWith(prefix)
      } and isTrue[String](s"id must contain non-empty digest after '$prefix'.") { id =>
        id.length > prefix.length
      }

    val validMesosEngineSpec: Validator[AppCContainer] = validator[AppCContainer] { appc =>
      appc.image is notEmpty
      appc.id is optional(validId)
    }
    validator{ (container: Container) =>
      container.appc is valid(definedAnd(validMesosEngineSpec))
    }
  }

  val mesosContainerValidator: Validator[Container] =
    // placeholder, there is no additional validation to do for a non-image-based mesos container
    new NullSafeValidator[Container](_ => true, _ => Failure(Set.empty))

  def validContainer(enabledFeatures: Set[String]): Validator[Container] = {
    def volumesValidator(container: Container): Validator[Seq[AppVolume]] =
      isTrue("Volume names must be unique") { (vols: Seq[AppVolume]) =>
        val names: Seq[String] = vols.flatMap(_.external.flatMap(_.name))
        names.distinct.size == names.size
      } and every(valid(validVolume(container, enabledFeatures)))

    val validGeneralContainer: Validator[Container] = validator[Container] { container =>
      container.portMappings is portMappingsValidator
      container.volumes is volumesValidator(container)
    } and valid(conditional[Container](_.`type` == EngineType.Docker)(dockerDockerContainerValidator))

    val validEngineWithImage = new Validator[Container] {
      override def apply(container: Container): Result = {
        (container.docker, container.appc, container.`type`) match {
          case (_, None, EngineType.Docker) => Success // handled by validGeneralContainer
          case (Some(_), None, EngineType.Mesos) => validate(container)(mesosDockerContainerValidator)
          case (None, Some(_), EngineType.Mesos) => validate(container)(mesosAppcContainerValidator)
          case (None, None, EngineType.Mesos) => validate(container)(mesosContainerValidator)
          case _ => Failure(Set(RuleViolation(container, "mesos containers should specify, at most, a single image type", None)))
        }
      }
    }
    valid(validGeneralContainer and validEngineWithImage)
  }

  def validVolume(container: Container, enabledFeatures: Set[String]): Validator[AppVolume] = new Validator[AppVolume] {
    import state.PathPatterns._
    val validHostVolume = validator[AppVolume] { v =>
      v.containerPath is valid(notEmpty)
      v.hostPath is valid(definedAnd(notEmpty))
    }
    val validPersistentVolume = {
      val notHaveConstraintsOnRoot = isTrue[PersistentVolume](
        "Constraints on root volumes are not supported") { info =>
          if (info.`type`.forall(_ == PersistentVolumeType.Root)) // default is Root, see AppConversion
            info.constraints.isEmpty
          else
            true
        }

      val meetMaxSizeConstraint = isTrue[PersistentVolume]("Only mount volumes can have maxSize") { info =>
        info.`type`.contains(PersistentVolumeType.Mount) || info.maxSize.isEmpty
      }

      val haveProperlyOrderedMaxSize = isTrue[PersistentVolume]("Max size must be larger than size") { info =>
        info.maxSize.forall(_ > info.size)
      }

      val complyWithVolumeConstraintRules: Validator[Seq[String]] = new Validator[Seq[String]] {
        override def apply(c: Seq[String]): Result = {
          import Protos.Constraint.Operator._
          (c.headOption, c.lift(1), c.lift(2)) match {
            case (None, None, _) =>
              Failure(Set(RuleViolation(c, "Missing field and operator", None)))
            case (Some("path"), Some(op), Some(value)) =>
              Try(Protos.Constraint.Operator.valueOf(op)).toOption.map {
                case LIKE | UNLIKE =>
                  Try(Pattern.compile(value)).toOption.map(_ => Success).getOrElse(
                    Failure(Set(RuleViolation(c, "Invalid regular expression", Some(value))))
                  )
                case _ =>
                  Failure(Set(
                    RuleViolation(c, "Operator must be one of LIKE, UNLIKE", None)))
              }.getOrElse(
                Failure(Set(
                  RuleViolation(c, s"unknown constraint operator $op", None)))
              )
            case _ =>
              Failure(Set(RuleViolation(c, s"Unsupported constraint ${c.mkString(",")}", None)))
          }
        }
      }

      val validPersistentInfo = validator[PersistentVolume] { info =>
        info.size should be > 0L
        info.constraints.each must complyWithVolumeConstraintRules
      } and meetMaxSizeConstraint and notHaveConstraintsOnRoot and haveProperlyOrderedMaxSize

      validator[AppVolume] { v =>
        v.containerPath is valid(notEqualTo("") and notOneOf(DotPaths: _*))
        v.containerPath is valid(matchRegexWithFailureMessage(NoSlashesPattern, "value must not contain \"/\""))
        v.mode is equalTo(ReadMode.Rw) // see AppConversion, default is RW
        v.persistent is valid(definedAnd(validPersistentInfo))
      }
    }
    val validExternalVolume: Validator[AppVolume] = {
      import state.OptionLabelPatterns._
      val validOptions = validator[Map[String, String]] { option =>
        option.keys.each should matchRegex(OptionKeyRegex)
      }
      val validExternalInfo: Validator[ExternalVolume] = validator[ExternalVolume] { info =>
        info.size should optional(be > 0L)
        info.name is valid(definedAnd(matchRegex(LabelRegex)))
        info.provider is valid(definedAnd(matchRegex(LabelRegex)))
        info.options is validOptions
      }

      validator[AppVolume] { v =>
        v.containerPath is valid(notEmpty)
        v.external is valid(definedAnd(validExternalInfo))
      } and conditional[AppVolume](_.external.exists(_.provider.nonEmpty))(ExternalVolumes.validRamlVolume(container)
      ) and featureEnabled[AppVolume](enabledFeatures, Features.EXTERNAL_VOLUMES)
    }
    override def apply(v: AppVolume): Result = {
      (v.persistent, v.external) match {
        case (None, None) => validate(v)(validHostVolume)
        case (Some(_), None) => validate(v)(validPersistentVolume)
        case (None, Some(_)) => validate(v)(validExternalVolume)
        case _ => Failure(Set(RuleViolation(v, "illegal combination of persistent and external volume fields", None)))
      }
    }
  }

  def readinessCheckValidator(app: App): Validator[ReadinessCheck] = {
    // we expect that the deprecated API has already been translated into canonical form
    val portNames = (app.portDefinitions.map(_.flatMap(_.name)).getOrElse(Nil) ++
      app.container.fold[Seq[String]](Seq.empty)(_.portMappings.flatMap(_.name))).to[Set]
    def portNameExists = isTrue[String]{ name: String => s"No port definition reference for portName $name" } { name =>
      portNames.contains(name)
    }
    validator[ReadinessCheck] { rc =>
      rc.name is notEmpty
      rc.path is notEmpty
      rc.portName is valid(portNameExists)
      rc.timeoutSeconds should be < rc.intervalSeconds
      rc.timeoutSeconds should (be > 0)
      rc.httpStatusCodesForReady is notEmpty
    }
  }

  val validDiscoveryPort: Validator[IpDiscoveryPort] = validator[IpDiscoveryPort] { port =>
    port.name is valid(matchRegex(portNameRegex))
    port.number is valid(validPortNumber)
  }

  val validDiscoveryInfo: Validator[IpDiscovery] = validator[IpDiscovery] { di =>
    di.ports is every(validDiscoveryPort)
  }

  val validIpAddress: Validator[IpAddress] = validator[IpAddress] { addr =>
    addr.networkName is optional(notEqualTo(""))
    addr.discovery is optional(validDiscoveryInfo)
  }

  /**
    * all validation that touches deprecated app-update API fields goes in here
    */
  def validateOldAppUpdateAPI: Validator[AppUpdate] = validator[AppUpdate] { update =>
    update.container.flatMap(_.docker.map(_.portMappings)) is optional(portMappingsValidator)
    update.ipAddress is optional(validIpAddress) and optional(isTrue(
      "ipAddress/discovery is not allowed for Docker containers") { (ipAddress: IpAddress) =>
        !(update.container.exists(c => c.`type` == EngineType.Docker) && ipAddress.discovery.nonEmpty)
      })
    update.ports is optional(every(validPortNumber))
    update.uris is optional(every(ArtifactValidation.uriValidator) and isTrue(
      "may not be set in conjunction with fetch"){ (uris: Seq[String]) => !(uris.nonEmpty && update.fetch.fold(false)(_.nonEmpty)) })
  } and isTrue("ports must be unique") { (update: AppUpdate) =>
    val withoutRandom = update.ports.fold(Seq.empty[Int])(_.filterNot(_ == AppDefinition.RandomPortValue))
    withoutRandom.distinct.size == withoutRandom.size
  } and isTrue("cannot specify both an IP address and port") { (update: AppUpdate) =>
    val appWithoutPorts = update.ports.fold(true)(_.isEmpty) && update.portDefinitions.fold(true)(_.isEmpty)
    appWithoutPorts || update.ipAddress.isEmpty
  } and isTrue("cannot specify both ports and port definitions") { (update: AppUpdate) =>
    val portDefinitionsIsEquivalentToPorts = update.portDefinitions.map(_.map(_.port)) == update.ports
    portDefinitionsIsEquivalentToPorts || update.ports.isEmpty || update.portDefinitions.isEmpty
  } and isTrue("must not specify both networks and ipAddress") { (update: AppUpdate) =>
    !(update.ipAddress.nonEmpty && update.networks.fold(false)(_.nonEmpty))
  } and isTrue("must not specify both container.docker.network and networks") { (update: AppUpdate) =>
    !(update.container.exists(_.docker.exists(_.network.nonEmpty)) && update.networks.nonEmpty)
  }

  def validateCanonicalAppUpdateAPI(enabledFeatures: Set[String]): Validator[AppUpdate] = validator[AppUpdate] { update =>
    update.id.map(PathId(_)) as "id" is optional(valid)
    update.executor.each should matchRegex(executorPattern)
    update.mem should optional(be >= 0.0)
    update.cpus should optional(be >= 0.0)
    update.instances should optional(be >= 0)
    update.disk should optional(be >= 0.0)
    update.gpus should optional(be >= 0)
    update.dependencies.map(_.map(PathId(_))) as "dependencies" is optional(every(valid))
    update.env is optional(envValidator(update.secrets.getOrElse(Map.empty), enabledFeatures))
    update.secrets is optional(conditional[Map[String, SecretDef]](_.nonEmpty)(featureEnabled(enabledFeatures, Features.SECRETS)))
    update.secrets is optional(implied[Map[String, SecretDef]](enabledFeatures.contains(Features.SECRETS))(every(secretEntryValidator)))
    update.storeUrls is optional(every(urlCanBeResolvedValidator))
    update.fetch is optional(every(valid))
    update.upgradeStrategy is optional(valid)
    update.residency is optional(valid)
    update.portDefinitions is optional(portDefinitionsValidator)
    update.container is optional(validContainer(enabledFeatures))
    update.acceptedResourceRoles is valid(optional(ResourceRole.validAcceptedResourceRoles(update.residency.isDefined) and notEmpty))
  } and isTrue("must not be root") { (update: AppUpdate) =>
    !update.id.fold(false)(PathId(_).isRoot)
  } and isTrue("must not be an empty string") { (update: AppUpdate) =>
    update.cmd.forall { s => s.length() > 1 }
  } and isTrue("portMappings are not allowed with host-networking") { (app: AppUpdate) =>
    !(app.networks.exists(_.exists(_.mode == NetworkMode.Host)) && app.container.exists(_.portMappings.nonEmpty))
  } and isTrue("portDefinitions are only allowed with host-networking") { (app: AppUpdate) =>
    !(app.networks.exists(_.exists(_.mode != NetworkMode.Host)) && app.portDefinitions.exists(_.nonEmpty))
  } and isTrue("The 'version' field may only be combined with the 'id' field.") { (app: AppUpdate) =>
    def onlyVersionOrIdSet: Boolean = app.productIterator.forall {
      case x: Some[Any] => x == app.version || x == app.id // linter:ignore UnlikelyEquality
      case _ => true
    }
    app.version.isEmpty || onlyVersionOrIdSet
  }

  /**
    * all validation that touches deprecated app API fields goes in here
    */
  val validateOldAppAPI: Validator[App] = validator[App] { app =>
    app.container.flatMap(_.docker.map(_.portMappings)) is optional(portMappingsValidator)
    app.ipAddress is optional(validIpAddress) and optional(isTrue(
      "ipAddress/discovery is not allowed for Docker containers") { (ipAddress: IpAddress) =>
        !(app.container.exists(c => c.`type` == EngineType.Docker) && ipAddress.discovery.nonEmpty)
      })
    app.ports is optional(every(validPortNumber))
    app.uris is optional(every(ArtifactValidation.uriValidator) and isTrue(
      "may not be set in conjunction with fetch"){ (uris: Seq[String]) => !(uris.nonEmpty && app.fetch.nonEmpty) })
  } and isTrue("must not specify both container.docker.network and networks") { (app: App) =>
    !(app.container.exists(_.docker.exists(_.network.nonEmpty)) && app.networks.nonEmpty)
  } and isTrue("must not specify both networks and ipAddress") { (app: App) =>
    !(app.ipAddress.nonEmpty && app.networks.nonEmpty)
  } and isTrue("ports must be unique") { (app: App) =>
    val withoutRandom: Seq[Int] = app.ports.map(_.filterNot(_ == AppDefinition.RandomPortValue)).getOrElse(Nil)
    withoutRandom.distinct.size == withoutRandom.size
  } and isTrue("cannot specify both an IP address and port") { (app: App) =>
    def appWithoutPorts = !(app.ports.exists(_.nonEmpty) || app.portDefinitions.exists(_.nonEmpty))
    app.ipAddress.isEmpty || appWithoutPorts
  } and isTrue("cannot specify both ports and port definitions") { (app: App) =>
    def portDefinitionsIsEquivalentToPorts = app.portDefinitions.map(_.map(_.port)) == app.ports
    app.ports.isEmpty || app.portDefinitions.isEmpty || portDefinitionsIsEquivalentToPorts
  }

  def validateCanonicalAppAPI(enabledFeatures: Set[String]): Validator[App] = validator[App] { app =>
    app.executor is valid(matchRegexFully(executorPattern))
    PathId(app.id) as "id" is (PathId.pathIdValidator and PathId.absolutePathValidator)
    app.dependencies.map(PathId(_)) as "dependencies" is every(PathId.validPathWithBase(PathId(app.id).parent))
  } and validBasicAppDefinition(enabledFeatures) and isTrue("must not be root") { (app: App) =>
    !PathId(app.id).isRoot
  } and isTrue("must not be an empty string") { (app: App) =>
    app.cmd.forall { s => s.length() > 1 }
  } and isTrue("portMappings are not allowed with host-networking") { (app: App) =>
    !(app.networks.exists(_.mode == NetworkMode.Host) && app.container.exists(_.portMappings.nonEmpty))
  } and isTrue("portDefinitions are only allowed with host-networking") { (app: App) =>
    !(app.networks.exists(_.mode != NetworkMode.Host) && app.portDefinitions.exists(_.nonEmpty))
  }

  /** expects that app is already in canonical form */
  def validNestedApp(base: PathId, enabledFeatures: Set[String]): Validator[App] = validator[App] { app =>
    PathId(app.id) as "id" is PathId.validPathWithBase(base)
  } and validBasicAppDefinition(enabledFeatures)

  /** should be kept in sync with [[AppDefinition.portIndices]] */
  def portIndices(app: App): Range = {
    app.container.withFilter(_.portMappings.nonEmpty)
      .map(_.portMappings).orElse(app.portDefinitions).getOrElse(Nil).indices
  }

  /** validate most canonical API fields */
  private def validBasicAppDefinition(enabledFeatures: Set[String]): Validator[App] = validator[App] { app =>
    app.upgradeStrategy is valid
    app.container is optional(validContainer(enabledFeatures))
    app.storeUrls is every(urlCanBeResolvedValidator)
    app.portDefinitions is optional(portDefinitionsValidator)
    app.executor should valid(matchRegexFully(executorPattern))
    app is containsCmdArgsOrContainer
    app.healthChecks is every(portIndexIsValid(portIndices(app)))
    app must haveAtMostOneMesosHealthCheck
    app.instances should valid(be >= 0)
    app.fetch is every(valid)
    app.mem should valid(be >= 0.0)
    app.cpus should valid(be >= 0.0)
    app.instances should valid(be >= 0)
    app.disk should valid(be >= 0.0)
    app.gpus should valid(be >= 0)
    app.secrets is valid(conditional[Map[String, SecretDef]](_.nonEmpty)(
      secretValidator and featureEnabled(enabledFeatures, Features.SECRETS)))
    app.env is envValidator(app.secrets, enabledFeatures)
    app.acceptedResourceRoles is valid(optional(ResourceRole.validAcceptedResourceRoles(app.residency.isDefined) and notEmpty))
    app must complyWithGpuRules(enabledFeatures)
    app must complyWithMigrationAPI
    app must complyWithReadinessCheckRules
    app must complyWithResidencyRules
    app must complyWithSingleInstanceLabelRules
    app must complyWithUpgradeStrategyRules
    app.constraints.each must complyWithConstraintRules
    app.networks is ramlNetworksValidator
    app.networks is every(valid)
  } // TODO(jdef) and ExternalVolumes.validApp

  val complyWithConstraintRules: Validator[Seq[String]] = new Validator[Seq[String]] {
    import Protos.Constraint.Operator._
    override def apply(c: Seq[String]): Result = {
      (c.headOption, c.lift(1), c.lift(2)) match {
        case (None, None, _) =>
          Failure(Set(RuleViolation(c, "Missing field and operator", None)))
        case (Some(field), Some(op), value) =>
          Try(Protos.Constraint.Operator.valueOf(op)) match {
            case scala.util.Success(operator) =>
              operator match {
                case UNIQUE =>
                  value.map(_ => Failure(Set(RuleViolation(c, "Value specified but not used", None)))).getOrElse(Success)
                case CLUSTER =>
                  value.map(_ => Success).getOrElse(Failure(Set(RuleViolation(c, "Missing value", None))))
                case GROUP_BY =>
                  value.fold[Result](Success){ v =>
                    Try(v.toInt).toOption.map(_ => Success).getOrElse(Failure(Set(RuleViolation(
                      c, "Value was specified but is not a number", Some("GROUP_BY may either have no value or an integer value")))))
                  }
                case LIKE | UNLIKE =>
                  value.map { v =>
                    Try(Pattern.compile(v)).toOption.map(_ => Success).getOrElse(
                      Failure(Set(RuleViolation(c, s"'$v' is not a valid regular expression", Some(s"$v")))))
                  }.getOrElse(
                    Failure(Set(RuleViolation(c, "A regular expression value must be provided", None)))
                  )
                case MAX_PER =>
                  value.fold[Result](Success){ v =>
                    Try(v.toInt).toOption.map(_ => Success).getOrElse(
                      Failure(Set(RuleViolation(c, "Value was specified but is not a number", Some("MAX_PER may have an integer value")))))
                  }
                case _ =>
                  Failure(Set(
                    RuleViolation(c, "Operator must be one of UNIQUE, CLUSTER, GROUP_BY, LIKE, MAX_PER or UNLIKE", None)))
              }
            case _ =>
              Failure(Set(RuleViolation(c, s"illegal constraint operator $op", None)))
          }
        case _ =>
          Failure(Set(RuleViolation(c, s"illegal constraint specification ${c.mkString(",")}", None)))
      }
    }
  }

  private val complyWithUpgradeStrategyRules: Validator[App] = validator[App] { app =>
    app.upgradeStrategy is optional(implied(isSingleInstance(app))(validForSingleInstanceApps))
    app.upgradeStrategy is optional(implied(app.residency.nonEmpty)(validForResidentTasks))
  }

  lazy val validForResidentTasks: Validator[UpgradeStrategy] = validator[UpgradeStrategy] { strategy =>
    strategy.minimumHealthCapacity is between(0.0, 1.0)
    strategy.maximumOverCapacity should valid(be == 0.0)
  }

  lazy val validForSingleInstanceApps: Validator[UpgradeStrategy] = validator[UpgradeStrategy] { strategy =>
    strategy.minimumHealthCapacity should valid(be == 0.0)
    strategy.maximumOverCapacity should valid(be == 0.0)
  }

  private val complyWithSingleInstanceLabelRules: Validator[App] =
    isTrue("Single instance app may only have one instance") { app =>
      (!isSingleInstance(app)) || (app.instances <= 1)
    }

  def isSingleInstance(app: App): Boolean = app.labels.get(AppDefinition.Labels.SingleInstanceApp).contains("true")

  private val complyWithResidencyRules: Validator[App] =
    isTrue("App must contain persistent volumes and define residency") { app =>
      val hasPersistentVolumes = app.container.fold(false)(_.volumes.exists(_.persistent.nonEmpty))
      !(app.residency.isDefined ^ hasPersistentVolumes)
    }

  private val complyWithReadinessCheckRules: Validator[App] = validator[App] { app =>
    app.readinessChecks.size should be <= 1
    app.readinessChecks is every(readinessCheckValidator(app))
  }

  private val complyWithMigrationAPI: Validator[App] =
    isTrue("DCOS_PACKAGE_FRAMEWORK_NAME and DCOS_MIGRATION_API_PATH must be defined" +
      " when using DCOS_MIGRATION_API_VERSION") { app =>
      val understandsMigrationProtocol = app.labels.get(AppDefinition.Labels.DcosMigrationApiVersion).exists(_.nonEmpty)

      // if the api version IS NOT set, we're ok
      // if the api version IS set, we expect to see a valid version, a frameworkName and a path
      def compliesWithMigrationApi =
        app.labels.get(AppDefinition.Labels.DcosMigrationApiVersion).fold(true) { apiVersion =>
          apiVersion == "v1" &&
            app.labels.get(AppDefinition.Labels.DcosPackageFrameworkName).exists(_.nonEmpty) &&
            app.labels.get(AppDefinition.Labels.DcosMigrationApiPath).exists(_.nonEmpty)
        }

      !understandsMigrationProtocol || (understandsMigrationProtocol && compliesWithMigrationApi)
    }

  private def complyWithGpuRules(enabledFeatures: Set[String]): Validator[App] =
    conditional[App](_.gpus > 0) {
      isTrue[App]("GPU resources only work with the Mesos containerizer") { app =>
        !app.container.exists(_.`type` == EngineType.Docker)
      } and featureEnabled(enabledFeatures, Features.GPU_RESOURCES)
    }

  private def portIndexIsValid(hostPortsIndices: Range): Validator[AppHealthCheck] = {
    val marathonProtocols = Set(AppHealthCheckProtocol.Http, AppHealthCheckProtocol.Https, AppHealthCheckProtocol.Tcp)
    isTrue("Health check port indices must address an element of the ports array or container port mappings.") { check =>
      if (check.command.isEmpty && marathonProtocols.contains(check.protocol)) {
        check.portIndex match {
          case Some(idx) => hostPortsIndices.contains(idx)
          case _ => check.port.nonEmpty || (hostPortsIndices.length == 1 && hostPortsIndices.headOption.contains(0))
        }
      } else {
        true
      }
    }
  }

  private val haveAtMostOneMesosHealthCheck: Validator[App] = {
    val mesosProtocols = Set(
      AppHealthCheckProtocol.Command,
      AppHealthCheckProtocol.MesosHttp,
      AppHealthCheckProtocol.MesosHttps,
      AppHealthCheckProtocol.MesosTcp)

    isTrue[App]("AppDefinition can contain at most one Mesos health check") { app =>
      val mesosCommandHealthChecks = app.healthChecks.count(_.command.nonEmpty)
      val allMesosHealthChecks = app.healthChecks.count { check =>
        check.command.nonEmpty || mesosProtocols.contains(check.protocol)
      }
      // Previous versions of Marathon allowed saving an app definition with more than one command health check, and
      // we don't want to make them invalid
      allMesosHealthChecks - mesosCommandHealthChecks <= 1
    }
  }

  private val containsCmdArgsOrContainer: Validator[App] =
    isTrue("AppDefinition must either contain one of 'cmd' or 'args', and/or a 'container'.") { app =>
      val cmd = app.cmd.nonEmpty
      val args = app.args.nonEmpty
      val container = app.container.exists { ct =>
        (ct.docker, ct.appc, ct.`type`) match {
          case (Some(_), None, EngineType.Docker) |
            (Some(_), None, EngineType.Mesos) |
            (None, Some(_), EngineType.Mesos) => true
          case _ => false
        }
      }
      (cmd ^ args) || (!(cmd && args) && container)
    }

}

object AppValidation extends AppValidation {

  val executorPattern = "^(//cmd)|(/?[^/]+(/[^/]+)*)|$".r
  val portNameRegex = "^[a-z][a-z0-9-]*$".r
}
