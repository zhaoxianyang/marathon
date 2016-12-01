package mesosphere.marathon
package raml

import mesosphere.UnitTest

class ContainerConversionTest extends UnitTest {

  "A Mesos Plain container is converted" when {
    "a mesos container" should {
      "convert to a RAML container" in {
        val container = state.Container.Mesos(Seq.empty)
        val raml = container.toRaml[Container]
        raml.`type` should be(EngineType.Mesos)
        raml.appc should be(empty)
        raml.docker should be(empty)
        raml.volumes should be(empty)
      }
    }
    "a RAML container" should {
      "convert to a mesos container" in {
        val container = Container(EngineType.Mesos, portMappings = Seq(ramlPortMapping))
        val mc = Some(container.fromRaml).collect {
          case c: state.Container.Mesos => c
        }.getOrElse(fail("expected Container.Mesos"))
        mc.portMappings should be(Seq(corePortMapping))
        mc.volumes should be(empty)
      }
    }
  }

  "A Mesos Docker container is converted" when {
    "a mesos-docker container" should {
      "convert to a RAML container" in {
        val container = state.Container.MesosDocker(Nil, "test", Nil, Some(credentials))
        val raml = container.toRaml[Container]
        raml.`type` should be(EngineType.Mesos)
        raml.appc should be(empty)
        raml.volumes should be(empty)
        raml.docker should be(defined)
        raml.docker.get.image should be("test")
        raml.docker.get.credential should be(defined)
        raml.docker.get.credential.get.principal should be(credentials.principal)
        raml.docker.get.credential.get.secret should be(credentials.secret)
      }
    }
    "a RAML container" should {
      "convert to a mesos-docker container" in {
        val container = Container(EngineType.Mesos, portMappings = Seq(ramlPortMapping), docker = Some(DockerContainer(
          image = "foo", credential = Some(DockerCredentials(credentials.principal, credentials.secret)))))
        val mc = Some(container.fromRaml).collect {
          case c: state.Container.MesosDocker => c
        }.getOrElse(fail("expected Container.MesosDocker"))
        mc.portMappings should be(Seq(corePortMapping))
        mc.volumes should be(empty)
        mc.image should be("foo")
        mc.credential should be(Some(credentials))
        mc.forcePullImage should be(container.docker.head.forcePullImage)
      }
    }
  }

  "A Mesos AppC container is created correctly" when {
    "a mesos-appc container" should {
      "convert to a RAML container" in {
        val container = state.Container.MesosAppC(Nil, "test", Nil, Some("id"))
        val raml = container.toRaml[Container]
        raml.`type` should be(EngineType.Mesos)
        raml.volumes should be(empty)
        raml.docker should be(empty)
        raml.appc should be(defined)
        raml.appc.get.image should be("test")
        raml.appc.get.id should be(Some("id"))
      }
    }
    "a RAML container" should {
      "convert to a mesos-appc container" in {
        val container = Container(
          EngineType.Mesos, portMappings = Seq(ramlPortMapping), appc = Some(AppCContainer(image = "foo")))
        val mc = Some(container.fromRaml).collect {
          case c: state.Container.MesosAppC => c
        }.getOrElse(fail("expected Container.MesosAppC"))
        mc.portMappings should be(Seq(corePortMapping))
        mc.volumes should be(empty)
        mc.image should be("foo")
        mc.forcePullImage should be(container.appc.head.forcePullImage)
        mc.id should be(container.appc.head.id)
        mc.labels should be(empty)
      }
    }
  }

  "A Docker Docker container is created correctly" when {
    "a docker-docker container" should {
      "convert to a RAML container" in {
        val portMapping = state.Container.PortMapping(23, Some(123), 0)
        val container = state.Container.Docker(Nil, "test", Seq(portMapping))
        val raml = container.toRaml[Container]
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
    }
    "a RAML container" should {
      "convert to a docker-docker container" in {
        val container = Container(EngineType.Docker, portMappings = Seq(ramlPortMapping), docker = Some(DockerContainer(
          image = "foo", parameters = Seq(DockerParameter("qws", "erf")))))
        val mc = Some(container.fromRaml).collect {
          case c: state.Container.Docker => c
        }.getOrElse(fail("expected Container.Docker"))
        mc.portMappings should be(Seq(corePortMapping))
        mc.volumes should be(empty)
        mc.image should be("foo")
        mc.forcePullImage should be(container.docker.head.forcePullImage)
        mc.parameters should be(Seq(state.Parameter("qws", "erf")))
        mc.privileged should be(container.docker.head.privileged)
      }
    }
  }

  private val credentials = state.Container.Credential("principal", Some("secret"))
  private val ramlPortMapping = ContainerPortMapping(
    containerPort = 80,
    hostPort = Some(90),
    servicePort = 100,
    name = Some("pok"),
    labels = Map("wer" -> "rty")
  )
  private val corePortMapping = state.Container.PortMapping(
    containerPort = 80,
    hostPort = Some(90),
    servicePort = 100,
    name = Some("pok"),
    labels = Map("wer" -> "rty")
  )
}
