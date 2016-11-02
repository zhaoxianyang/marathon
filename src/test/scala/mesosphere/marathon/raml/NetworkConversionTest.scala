package mesosphere.marathon
package raml

import mesosphere.FunTest
import mesosphere.marathon.state.Container.PortMapping

class NetworkConversionTest extends FunTest {

  test("protocol is converted correctly") {
    "tcp".toRaml[NetworkProtocol] should be(NetworkProtocol.Tcp)
    "udp".toRaml[NetworkProtocol] should be(NetworkProtocol.Udp)
    "udp,tcp".toRaml[NetworkProtocol] should be(NetworkProtocol.UdpTcp)
  }

  test("port definition is converted correctly") {
    val portDefinition = state.PortDefinition(23, "udp", Some("test"), Map("foo" -> "bla"))
    val raml = portDefinition.toRaml[PortDefinition]
    raml.port should be(portDefinition.port)
    raml.name should be(portDefinition.name)
    raml.labels should be(portDefinition.labels)
    raml.protocol should be(NetworkProtocol.Udp)
  }

  test("port mappings is converted correctly") {
    val portMapping = PortMapping(23, Some(123), 0, "udp", Some("name"), Map("foo" -> "bla"))
    val raml = portMapping.toRaml[ContainerPortMapping]
    raml.containerPort should be(portMapping.containerPort)
    raml.hostPort should be(portMapping.hostPort)
    raml.servicePort should be(portMapping.servicePort)
    raml.name should be(portMapping.name)
    raml.labels should be(portMapping.labels)
    raml.protocol should be(NetworkProtocol.Udp)
  }
}
