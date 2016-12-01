package mesosphere.marathon
package raml

import mesosphere.marathon.core.pod.MesosContainer
import mesosphere.marathon.state.Parameter
import org.apache.mesos.{ Protos => Mesos }

trait ContainerConversion extends HealthCheckConversion with VolumeConversion {

  implicit val containerRamlWrites: Writes[MesosContainer, PodContainer] = Writes { c =>
    PodContainer(
      name = c.name,
      exec = c.exec,
      resources = c.resources,
      endpoints = c.endpoints,
      image = c.image,
      environment = Raml.toRaml(c.env),
      user = c.user,
      healthCheck = c.healthCheck.toRaml[Option[HealthCheck]],
      volumeMounts = c.volumeMounts,
      artifacts = c.artifacts,
      labels = c.labels,
      lifecycle = c.lifecycle
    )
  }

  implicit val containerRamlReads: Reads[PodContainer, MesosContainer] = Reads { c =>
    MesosContainer(
      name = c.name,
      exec = c.exec,
      resources = c.resources,
      endpoints = c.endpoints,
      image = c.image,
      env = Raml.fromRaml(c.environment),
      user = c.user,
      healthCheck = c.healthCheck.map(Raml.fromRaml(_)),
      volumeMounts = c.volumeMounts,
      artifacts = c.artifacts,
      labels = c.labels,
      lifecycle = c.lifecycle
    )
  }

  implicit val parameterWrites: Writes[state.Parameter, DockerParameter] = Writes { param =>
    DockerParameter(param.key, param.value)
  }

  implicit val containerWrites: Writes[state.Container, Container] = Writes { container =>

    implicit val credentialWrites: Writes[state.Container.Credential, DockerCredentials] = Writes { credentials =>
      DockerCredentials(credentials.principal, credentials.secret)
    }

    import Mesos.ContainerInfo.DockerInfo.{ Network => DockerNetworkMode }
    implicit val dockerNetworkInfoWrites: Writes[DockerNetworkMode, DockerNetwork] = Writes {
      case DockerNetworkMode.BRIDGE => DockerNetwork.Bridge
      case DockerNetworkMode.HOST => DockerNetwork.Host
      case DockerNetworkMode.USER => DockerNetwork.User
      case DockerNetworkMode.NONE => DockerNetwork.None
    }

    implicit val dockerDockerContainerWrites: Writes[state.Container.Docker, DockerContainer] = Writes { container =>
      DockerContainer(
        forcePullImage = container.forcePullImage,
        image = container.image,
        parameters = container.parameters.toRaml,
        privileged = container.privileged)
    }

    implicit val mesosDockerContainerWrites: Writes[state.Container.MesosDocker, DockerContainer] = Writes { container =>
      DockerContainer(
        image = container.image,
        credential = container.credential.toRaml,
        forcePullImage = container.forcePullImage)
    }

    implicit val mesosContainerWrites: Writes[state.Container.MesosAppC, AppCContainer] = Writes { container =>
      AppCContainer(container.image, container.id, container.labels, container.forcePullImage)
    }

    def create(kind: EngineType, docker: Option[DockerContainer] = None, appc: Option[AppCContainer] = None): Container = {
      Container(kind, docker = docker, appc = appc, volumes = container.volumes.toRaml, portMappings = container.portMappings.toRaml)
    }

    container match {
      case docker: state.Container.Docker => create(EngineType.Docker, docker = Some(docker.toRaml[DockerContainer]))
      case mesos: state.Container.MesosDocker => create(EngineType.Mesos, docker = Some(mesos.toRaml[DockerContainer]))
      case mesos: state.Container.MesosAppC => create(EngineType.Mesos, appc = Some(mesos.toRaml[AppCContainer]))
      case mesos: state.Container.Mesos => create(EngineType.Mesos)
    }
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

  implicit val appContainerRamlReader: Reads[Container, state.Container] = Reads { container =>
    val volumes = container.volumes.map(Raml.fromRaml(_))
    val portMappings = container.portMappings.map(Raml.fromRaml(_))

    val result: state.Container = (container.`type`, container.docker, container.appc) match {
      case (EngineType.Docker, Some(docker), None) =>
        state.Container.Docker(
          volumes = volumes,
          image = docker.image,
          portMappings = portMappings, // assumed already normalized, see AppNormalization
          privileged = docker.privileged,
          parameters = docker.parameters.map(p => Parameter(p.key, p.value)),
          forcePullImage = docker.forcePullImage
        )
      case (EngineType.Mesos, Some(docker), None) =>
        state.Container.MesosDocker(
          volumes = volumes,
          image = docker.image,
          portMappings = portMappings, // assumed already normalized, see AppNormalization
          credential = docker.credential.map(c => state.Container.Credential(principal = c.principal, secret = c.secret)),
          forcePullImage = docker.forcePullImage
        )
      case (EngineType.Mesos, None, Some(appc)) =>
        state.Container.MesosAppC(
          volumes = volumes,
          image = appc.image,
          portMappings = portMappings,
          id = appc.id,
          labels = appc.labels,
          forcePullImage = appc.forcePullImage
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
}

object ContainerConversion extends ContainerConversion
