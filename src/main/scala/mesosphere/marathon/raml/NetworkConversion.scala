package mesosphere.marathon
package raml

import mesosphere.marathon.core.pod

trait NetworkConversion {

  implicit val networkRamlReader: Reads[Network, pod.Network] =
    Reads { raml =>
      raml.mode match {
        case NetworkMode.Host => pod.HostNetwork
        case NetworkMode.ContainerBridge => pod.BridgeNetwork(raml.labels)
        case NetworkMode.Container => pod.ContainerNetwork(
          // TODO(PODS): shouldn't this be caught by validation?
          raml.name.getOrElse(throw new IllegalArgumentException("container network must specify a name")),
          raml.labels
        )
      }
    }

  implicit val networkRamlWriter: Writes[pod.Network, Network] = Writes {
    case cnet: pod.ContainerNetwork =>
      Network(
        name = Some(cnet.name),
        mode = NetworkMode.Container,
        labels = cnet.labels
      )
    case br: pod.BridgeNetwork =>
      Network(
        mode = NetworkMode.ContainerBridge,
        labels = br.labels
      )
    case pod.HostNetwork => Network(mode = NetworkMode.Host)
  }

  implicit val protocolWrites: Writes[String, NetworkProtocol] = Writes {
    case "tcp" => NetworkProtocol.Tcp
    case "udp" => NetworkProtocol.Udp
    case "udp,tcp" | "udp,tcp" => NetworkProtocol.UdpTcp
  }

  implicit val portDefinitionWrites: Writes[state.PortDefinition, PortDefinition] = Writes { port =>
    PortDefinition(port.port, port.labels, port.name, port.protocol.toRaml[NetworkProtocol])
  }

  implicit val portMappingWrites: Writes[state.Container.PortMapping, ContainerPortMapping] = Writes { portMapping =>
    ContainerPortMapping(
      containerPort = portMapping.containerPort,
      hostPort = portMapping.hostPort,
      labels = portMapping.labels,
      name = portMapping.name,
      protocol = portMapping.protocol.toRaml[NetworkProtocol],
      servicePort = portMapping.servicePort
    )
  }
}

object NetworkConversion extends NetworkConversion
