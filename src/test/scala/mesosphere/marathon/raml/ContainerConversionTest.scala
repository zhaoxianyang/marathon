package mesosphere.marathon
package raml

import mesosphere.FunTest

class ContainerConversionTest extends FunTest {

  test("A Mesos Plain container is created correctly") {
    Given("A mesos container")
    val container = state.Container.Mesos(Seq.empty)

    When("The container is converted")
    val raml = container.toRaml[Container]

    Then("The raml container is correct")
    raml.`type` should be(EngineType.Mesos)
    raml.appc should be(empty)
    raml.docker should be(empty)
    raml.volumes should be(empty)
  }

  test("A Mesos Docker container is created correctly") {
    Given("A mesos container")
    val container = state.Container.MesosDocker(Nil, "test", Nil, Some(credentials))

    When("The container is converted")
    val raml = container.toRaml[Container]

    Then("The raml container is correct")
    raml.`type` should be(EngineType.Mesos)
    raml.appc should be(empty)
    raml.volumes should be(empty)
    raml.docker should be(defined)
    raml.docker.get.image should be("test")
    raml.docker.get.credential should be(defined)
    raml.docker.get.credential.get.principal should be(credentials.principal)
    raml.docker.get.credential.get.secret should be(credentials.secret)
  }

  test("A Mesos AppC container is created correctly") {
    Given("A mesos container")
    val container = state.Container.MesosAppC(Nil, "test", Nil, Some("id"))

    When("The container is converted")
    val raml = container.toRaml[Container]

    Then("The raml container is correct")
    raml.`type` should be(EngineType.Mesos)
    raml.volumes should be(empty)
    raml.docker should be(empty)
    raml.appc should be(defined)
    raml.appc.get.image should be("test")
    raml.appc.get.id should be(Some("id"))
  }

  test("A Docker Docker container is created correctly") {
    Given("A mesos container")
    val portMapping = state.Container.PortMapping(23, Some(123), 0)
    val container = state.Container.Docker(Nil, "test", Seq(portMapping))

    When("The container is converted")
    val raml = container.toRaml[Container]

    Then("The raml container is correct")
    raml.`type` should be(EngineType.Docker)
    raml.appc should be(empty)
    raml.volumes should be(empty)
    raml.docker should be(defined)
    raml.docker.get.image should be("test")
    raml.docker.get.credential should be(empty)
    raml.docker.get.network should be(empty)
    raml.portMappings should have size 1
    val mapping = raml.portMappings.head
    mapping.containerPort should be(portMapping.containerPort)
    mapping.hostPort should be(portMapping.hostPort)
    mapping.servicePort should be(portMapping.servicePort)
  }

  val credentials = state.Container.Credential("principal", Some("secret"))
}
