package mesosphere.marathon
package core.health

import com.wix.accord.validate
import mesosphere.marathon.Protos.HealthCheckDefinition.Protocol
import mesosphere.marathon.api.JsonTestHelper
import mesosphere.marathon.api.v2.ValidationHelper
import mesosphere.marathon.core.pod.{ BridgeNetwork, ContainerNetwork, HostNetwork }
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.raml.{ AppHealthCheck, Raml }
import mesosphere.marathon.state.Container.PortMapping
import mesosphere.marathon.state._
import mesosphere.marathon.test.{ MarathonSpec, MarathonTestHelper }
import mesosphere.mesos.{ ResourceMatchResponse, RunSpecOfferMatcher, TaskBuilder }
import org.apache.mesos.{ Protos => MesosProtos }
import play.api.libs.json.{ Json, Writes }

import scala.collection.immutable.Seq
import scala.concurrent.duration._

class MesosHealthCheckTest extends MarathonSpec {

  implicit val healthCheckWrites: Writes[HealthCheck] = Writes { check =>
    val appCheck: AppHealthCheck = Raml.toRaml(check)
    AppHealthCheck.playJsonWriter.writes(appCheck)
  }

  // COMMAND health check
  test("Read COMMAND health check") {
    val json =
      """
        {
          "protocol": "COMMAND",
          "command": { "value": "echo healthy" },
          "gracePeriodSeconds": 300,
          "intervalSeconds": 60,
          "timeoutSeconds": 20,
          "maxConsecutiveFailures": 3,
          "delaySeconds": 15
        }
      """
    val expected = MesosCommandHealthCheck(command = Command("echo healthy"))
    val readResult = fromJson(json)
    assert(readResult == expected)
  }

  test("Write COMMAND health check") {
    val json =
      """
        {
          "protocol": "COMMAND",
          "command": { "value": "echo healthy" },
          "gracePeriodSeconds": 300,
          "intervalSeconds": 60,
          "timeoutSeconds": 20,
          "maxConsecutiveFailures": 3,
          "delaySeconds": 15
        }
      """
    JsonTestHelper.assertThatJsonOf(MesosCommandHealthCheck(command = Command("echo healthy")))
      .correspondsToJsonString(json)
  }

  test("Read COMMAND health check (portIndex may be provided for backwards-compatibility)") {
    val json =
      """
        {
          "protocol": "COMMAND",
          "command": { "value": "echo healthy" },
          "portIndex": 0,
          "gracePeriodSeconds": 300,
          "intervalSeconds": 60,
          "timeoutSeconds": 20,
          "maxConsecutiveFailures": 3,
          "delaySeconds": 15
        }
      """
    val expected = MesosCommandHealthCheck(command = Command("echo healthy"))
    val readResult = fromJson(json)
    assert(readResult == expected)
  }

  // Mesos HTTP[S] health check
  test("Read Mesos HTTP health check") {
    val portIndexJson =
      """
        {
          "path": "/health",
          "protocol": "MESOS_HTTP",
          "portIndex": 0,
          "gracePeriodSeconds": 10,
          "intervalSeconds": 60,
          "timeoutSeconds": 20,
          "maxConsecutiveFailures": 0,
          "delaySeconds": 15
        }
      """
    assert(fromJson(portIndexJson) == mesosHttpHealthCheckWithPortIndex)

    val portJson =
      """
        {
          "path": "/health",
          "protocol": "MESOS_HTTP",
          "port": 80,
          "gracePeriodSeconds": 10,
          "intervalSeconds": 60,
          "timeoutSeconds": 20,
          "maxConsecutiveFailures": 0,
          "delaySeconds": 15
        }
      """
    assert(fromJson(portJson) == mesosHttpHealthCheckWithPort)
  }

  test("Write Mesos HTTP health check") {
    val portIndexJson =
      """
        {
          "protocol": "MESOS_HTTP",
          "path": "/health",
          "portIndex": 0,
          "gracePeriodSeconds": 10,
          "intervalSeconds": 60,
          "timeoutSeconds": 20,
          "maxConsecutiveFailures": 0,
          "delaySeconds": 15
        }
      """
    JsonTestHelper.assertThatJsonOf(mesosHttpHealthCheckWithPortIndex)
      .correspondsToJsonString(portIndexJson)

    val portJson =
      """
        {
          "protocol": "MESOS_HTTP",
          "path": "/health",
          "port": 80,
          "gracePeriodSeconds": 10,
          "intervalSeconds": 60,
          "timeoutSeconds": 20,
          "maxConsecutiveFailures": 0,
          "delaySeconds": 15
        }
      """
    JsonTestHelper.assertThatJsonOf(mesosHttpHealthCheckWithPort)
      .correspondsToJsonString(portJson)
  }

  test("Read Mesos HTTPS health check") {
    val portIndexJson =
      """
        {
          "protocol": "MESOS_HTTPS",
          "path": "/health",
          "portIndex": 0,
          "gracePeriodSeconds": 10,
          "intervalSeconds": 60,
          "timeoutSeconds": 20,
          "maxConsecutiveFailures": 0,
          "delaySeconds": 15
        }
      """
    assert(fromJson(portIndexJson) == mesosHttpHealthCheckWithPortIndex.copy(protocol = Protocol.MESOS_HTTPS))

    val portJson =
      """
        {
          "protocol": "MESOS_HTTPS",
          "path": "/health",
          "port": 80,
          "gracePeriodSeconds": 10,
          "intervalSeconds": 60,
          "timeoutSeconds": 20,
          "maxConsecutiveFailures": 0,
          "delaySeconds": 15
        }
      """
    assert(fromJson(portJson) == mesosHttpHealthCheckWithPort.copy(protocol = Protocol.MESOS_HTTPS))
  }

  test("Write Mesos HTTPS health check") {
    val portIndexJson =
      """
        {
          "protocol": "MESOS_HTTPS",
          "path": "/health",
          "portIndex": 0,
          "gracePeriodSeconds": 10,
          "intervalSeconds": 60,
          "timeoutSeconds": 20,
          "maxConsecutiveFailures": 0,
          "delaySeconds": 15
        }
      """
    JsonTestHelper.assertThatJsonOf(mesosHttpHealthCheckWithPortIndex.copy(protocol = Protocol.MESOS_HTTPS))
      .correspondsToJsonString(portIndexJson)

    val portJson =
      """
        {
          "protocol": "MESOS_HTTPS",
          "path": "/health",
          "port": 80,
          "gracePeriodSeconds": 10,
          "intervalSeconds": 60,
          "timeoutSeconds": 20,
          "maxConsecutiveFailures": 0,
          "delaySeconds": 15
        }
      """
    JsonTestHelper.assertThatJsonOf(mesosHttpHealthCheckWithPort.copy(protocol = Protocol.MESOS_HTTPS))
      .correspondsToJsonString(portJson)
  }

  test("both port and portIndex are not accepted at the same time for a Mesos HTTP HealthCheck") {
    shouldBeInvalid(MesosHttpHealthCheck(
      port = Some(1),
      portIndex = Some(PortReference(0))
    ))
  }

  test("port is accepted for a Mesos HTTP HealthCheck") {
    shouldBeValid(MesosHttpHealthCheck(port = Some(1)))
  }

  test("portIndex is accepted for a Mesos HTTP HealthCheck") {
    shouldBeValid(MesosHttpHealthCheck(portIndex = Some(PortReference(0))))
  }

  test("ToProto Mesos HTTP HealthCheck with portIndex") {
    val healthCheck = MesosHttpHealthCheck(
      path = Some("/health"),
      protocol = Protocol.MESOS_HTTP,
      portIndex = Some(PortReference(0)),
      gracePeriod = 10.seconds,
      interval = 60.seconds,
      maxConsecutiveFailures = 0
    )

    val proto = healthCheck.toProto

    assert("/health" == proto.getPath)
    assert(Protocol.MESOS_HTTP == proto.getProtocol)
    assert(0 == proto.getPortIndex)
    assert(10 == proto.getGracePeriodSeconds)
    assert(60 == proto.getIntervalSeconds)
    assert(0 == proto.getMaxConsecutiveFailures)
    assert(!proto.hasPort)
  }

  test("ToProto Mesos HTTPS HealthCheck with port") {
    val healthCheck = MesosHttpHealthCheck(
      path = Some("/health"),
      protocol = Protocol.MESOS_HTTPS,
      gracePeriod = 10.seconds,
      interval = 60.seconds,
      maxConsecutiveFailures = 0,
      port = Some(12345)
    )

    val proto = healthCheck.toProto

    assert("/health" == proto.getPath)
    assert(Protocol.MESOS_HTTPS == proto.getProtocol)
    assert(!proto.hasPortIndex)
    assert(10 == proto.getGracePeriodSeconds)
    assert(60 == proto.getIntervalSeconds)
    assert(0 == proto.getMaxConsecutiveFailures)
    assert(12345 == proto.getPort)
  }

  test("ToProto Mesos TCP HealthCheck with port") {
    val healthCheck = MesosTcpHealthCheck(
      port = Some(80),
      gracePeriod = 7.seconds,
      interval = 35.seconds,
      maxConsecutiveFailures = 10
    )

    val proto = healthCheck.toProto

    assert(Protocol.MESOS_TCP == proto.getProtocol)
    assert(80 == proto.getPort)
    assert(7 == proto.getGracePeriodSeconds)
    assert(35 == proto.getIntervalSeconds)
    assert(10 == proto.getMaxConsecutiveFailures)
  }

  test("FromProto Mesos HTTP HealthCheck with portIndex") {
    val proto = Protos.HealthCheckDefinition.newBuilder
      .setPath("/health")
      .setProtocol(Protocol.MESOS_HTTP)
      .setPortIndex(0)
      .setGracePeriodSeconds(10)
      .setIntervalSeconds(60)
      .setTimeoutSeconds(10)
      .setMaxConsecutiveFailures(10)
      .build

    val mergeResult = HealthCheck.fromProto(proto)

    val expectedResult = MesosHttpHealthCheck(
      path = Some("/health"),
      protocol = Protocol.MESOS_HTTP,
      portIndex = Some(PortReference(0)),
      gracePeriod = 10.seconds,
      interval = 60.seconds,
      timeout = 10.seconds,
      maxConsecutiveFailures = 10,
      port = None
    )

    assert(mergeResult == expectedResult)
  }

  test("FromProto Mesos HTTPS HealthCheck with port") {
    val proto = Protos.HealthCheckDefinition.newBuilder
      .setPath("/health")
      .setProtocol(Protocol.MESOS_HTTPS)
      .setGracePeriodSeconds(10)
      .setIntervalSeconds(60)
      .setTimeoutSeconds(10)
      .setMaxConsecutiveFailures(10)
      .setPort(12345)
      .build

    val mergeResult = HealthCheck.fromProto(proto)

    val expectedResult = MesosHttpHealthCheck(
      path = Some("/health"),
      protocol = Protocol.MESOS_HTTPS,
      portIndex = None,
      gracePeriod = 10.seconds,
      interval = 60.seconds,
      timeout = 10.seconds,
      maxConsecutiveFailures = 10,
      port = Some(12345)
    )

    assert(mergeResult == expectedResult)
  }

  test("FromProto Mesos HTTP HealthCheck with neither port nor portIndex") {
    val proto = Protos.HealthCheckDefinition.newBuilder
      .setPath("/health")
      .setProtocol(Protocol.MESOS_HTTP)
      .setGracePeriodSeconds(10)
      .setIntervalSeconds(60)
      .setTimeoutSeconds(10)
      .setMaxConsecutiveFailures(10)
      .build

    val mergeResult = HealthCheck.fromProto(proto)

    val expectedResult = MesosHttpHealthCheck(
      path = Some("/health"),
      protocol = Protocol.MESOS_HTTP,
      portIndex = Some(PortReference(0)),
      gracePeriod = 10.seconds,
      interval = 60.seconds,
      timeout = 10.seconds,
      maxConsecutiveFailures = 10,
      port = None
    )

    assert(mergeResult == expectedResult)
  }

  test("FromProto Mesos HTTPS HealthCheck") {
    val proto = Protos.HealthCheckDefinition.newBuilder
      .setPath("/health")
      .setProtocol(Protocol.MESOS_HTTPS)
      .setPortIndex(0)
      .setGracePeriodSeconds(10)
      .setIntervalSeconds(60)
      .setTimeoutSeconds(10)
      .setMaxConsecutiveFailures(10)
      .build

    val mergeResult = HealthCheck.fromProto(proto)

    val expectedResult = MesosHttpHealthCheck(
      path = Some("/health"),
      protocol = Protocol.MESOS_HTTPS,
      portIndex = Some(PortReference(0)),
      gracePeriod = 10.seconds,
      interval = 60.seconds,
      timeout = 10.seconds,
      maxConsecutiveFailures = 10
    )

    assert(mergeResult == expectedResult)
  }

  test("Mesos HTTP HealthCheck toMesos with host networking and portIndex") {
    import mesosphere.marathon.test.MarathonTestHelper.Implicits._

    val app = MarathonTestHelper.makeBasicApp().withHealthCheck(mesosHttpHealthCheckWithPortIndex)

    val task: Option[(MesosProtos.TaskInfo, Seq[Option[Int]])] = buildIfMatches(app)
    assert(task.isDefined)

    val (taskInfo, taskPorts) = task.get
    assertHttpHealthCheckProto(taskInfo, taskPorts.head.get, "http")
  }

  test("Mesos HTTPS HealthCheck toMesos with host networking and portIndex") {
    import MarathonTestHelper.Implicits._

    val healthCheck = MesosHttpHealthCheck(
      path = Some("/health"),
      protocol = Protocol.MESOS_HTTPS,
      portIndex = Some(PortReference(0)),
      gracePeriod = 10.seconds,
      interval = 60.seconds,
      maxConsecutiveFailures = 0)

    val app = MarathonTestHelper.makeBasicApp().withHealthCheck(healthCheck)

    val task: Option[(MesosProtos.TaskInfo, Seq[Option[Int]])] = buildIfMatches(app)
    assert(task.isDefined)

    val (taskInfo, taskPorts) = task.get
    assertHttpHealthCheckProto(taskInfo, taskPorts.head.get, "https")
  }

  test("Mesos HTTP HealthCheck toMesos with Docker HOST networking and portIndex") {
    import MarathonTestHelper.Implicits._

    val app = MarathonTestHelper.makeBasicApp()
      .withDockerNetworks(HostNetwork)
      .withHealthCheck(mesosHttpHealthCheckWithPortIndex)

    val task: Option[(MesosProtos.TaskInfo, Seq[Option[Int]])] = buildIfMatches(app)
    assert(task.isDefined)

    val (taskInfo, taskPorts) = task.get
    assertHttpHealthCheckProto(taskInfo, taskPorts.head.get, "http")
  }

  test("Mesos HTTP HealthCheck toMesos with Docker HOST networking and port") {
    import MarathonTestHelper.Implicits._

    val app = MarathonTestHelper.makeBasicApp()
      .withDockerNetworks(HostNetwork)
      .withHealthCheck(mesosHttpHealthCheckWithPort)

    val task: Option[(MesosProtos.TaskInfo, Seq[Option[Int]])] = buildIfMatches(app)
    assert(task.isDefined)

    val (taskInfo, _) = task.get
    assertHttpHealthCheckProto(taskInfo, 80, "http")
  }

  test("Mesos HTTP HealthCheck toMesos with Docker BRIDGE networking and portIndex") {
    import MarathonTestHelper.Implicits._

    val app = MarathonTestHelper.makeBasicApp()
      .withNoPortDefinitions()
      .withDockerNetworks(BridgeNetwork())
      .withPortMappings(Seq(
        PortMapping(containerPort = 80, hostPort = Some(0), servicePort = 0, protocol = "tcp",
          name = Some("http"))
      ))
      .withHealthCheck(mesosHttpHealthCheckWithPortIndex)

    val task: Option[(MesosProtos.TaskInfo, Seq[Option[Int]])] = buildIfMatches(app)
    assert(task.isDefined)

    val (taskInfo, _) = task.get
    assertHttpHealthCheckProto(taskInfo, 80, "http")
  }

  test("Mesos HTTP HealthCheck toMesos with Docker BRIDGE networking and port") {
    import MarathonTestHelper.Implicits._

    val app = MarathonTestHelper.makeBasicApp()
      .withNoPortDefinitions()
      .withDockerNetworks(BridgeNetwork())
      .withPortMappings(Seq(
        PortMapping(containerPort = 8080, hostPort = Some(0), servicePort = 0, protocol = "tcp",
          name = Some("http"))
      ))
      .withHealthCheck(mesosHttpHealthCheckWithPort)

    val task: Option[(MesosProtos.TaskInfo, Seq[Option[Int]])] = buildIfMatches(app)
    assert(task.isDefined)

    val (taskInfo, _) = task.get
    assertHttpHealthCheckProto(taskInfo, 80, "http")
  }

  test("Mesos HTTP HealthCheck toMesos with Docker USER networking and a port mapping NOT requesting a host port, with portIndex") {
    import MarathonTestHelper.Implicits._

    val app = MarathonTestHelper.makeBasicApp()
      .withNoPortDefinitions()
      .withDockerNetworks(ContainerNetwork("whatever"))
      .withPortMappings(Seq(PortMapping(containerPort = 80, hostPort = None)))
      .withHealthCheck(mesosHttpHealthCheckWithPortIndex)

    val task: Option[(MesosProtos.TaskInfo, Seq[Option[Int]])] = buildIfMatches(app)
    assert(task.isDefined)

    val (taskInfo, _) = task.get
    assertHttpHealthCheckProto(taskInfo, 80, "http")
  }

  test("Mesos HTTP HealthCheck toMesos with Docker USER networking and a port mapping requesting a host port, with portIndex") {
    import MarathonTestHelper.Implicits._
    val app = MarathonTestHelper.makeBasicApp()
      .withNoPortDefinitions()
      .withDockerNetworks(ContainerNetwork("whatever"))
      .withPortMappings(Seq(PortMapping(containerPort = 80, hostPort = Some(0))))
      .withHealthCheck(mesosHttpHealthCheckWithPortIndex)

    val task: Option[(MesosProtos.TaskInfo, Seq[Option[Int]])] = buildIfMatches(app)
    assert(task.isDefined)

    val (taskInfo, _) = task.get
    assertHttpHealthCheckProto(taskInfo, 80, "http")
  }

  test("Mesos HTTP HealthCheck toMesos with Docker USER networking with port") {
    import MarathonTestHelper.Implicits._

    val app = MarathonTestHelper.makeBasicApp()
      .withNoPortDefinitions()
      .withDockerNetworks(ContainerNetwork("whatever"))
      .withPortMappings(Seq(PortMapping(containerPort = 31337, hostPort = Some(0))))
      .withHealthCheck(mesosHttpHealthCheckWithPort)

    val task: Option[(MesosProtos.TaskInfo, Seq[Option[Int]])] = buildIfMatches(app)
    assert(task.isDefined)

    val (taskInfo, _) = task.get
    assertHttpHealthCheckProto(taskInfo, 80, "http")
  }

  // Mesos TCP health check
  test("both port and portIndex are not accepted at the same time for a Mesos TCP HealthCheck") {
    shouldBeInvalid(MesosTcpHealthCheck(
      port = Some(1),
      portIndex = Some(PortReference(0))
    ))
  }

  test("port is accepted for a Mesos TCP HealthCheck") {
    shouldBeValid(MesosTcpHealthCheck(port = Some(1)))
  }

  test("portIndex is accepted for a Mesos TCP HealthCheck") {
    shouldBeValid(MesosTcpHealthCheck(portIndex = Some(PortReference(0))))
  }

  test("ToProto Mesos TCP HealthCheck with portIndex") {
    val healthCheck = MesosTcpHealthCheck(
      portIndex = Some(PortReference(1)),
      gracePeriod = 7.seconds,
      interval = 35.seconds,
      maxConsecutiveFailures = 10
    )

    val proto = healthCheck.toProto

    assert(Protocol.MESOS_TCP == proto.getProtocol)
    assert(1 == proto.getPortIndex)
    assert(7 == proto.getGracePeriodSeconds)
    assert(35 == proto.getIntervalSeconds)
    assert(10 == proto.getMaxConsecutiveFailures)
  }

  test("FromProto Mesos TCP HealthCheck with portIndex") {
    val proto = Protos.HealthCheckDefinition.newBuilder
      .setProtocol(Protocol.MESOS_TCP)
      .setPortIndex(1)
      .setGracePeriodSeconds(7)
      .setIntervalSeconds(35)
      .setTimeoutSeconds(10)
      .setMaxConsecutiveFailures(10)
      .build

    val mergeResult = HealthCheck.fromProto(proto)

    val expectedResult = MesosTcpHealthCheck(
      portIndex = Some(PortReference(1)),
      gracePeriod = 7.seconds,
      interval = 35.seconds,
      timeout = 10.seconds,
      maxConsecutiveFailures = 10
    )

    assert(mergeResult == expectedResult)
  }

  test("Mesos TCP HealthCheck toMesos with host networking and portIndex") {
    import MarathonTestHelper.Implicits._

    val app = MarathonTestHelper.makeBasicApp().withHealthCheck(mesosTcpHealthCheckWithPortIndex)

    val task: Option[(MesosProtos.TaskInfo, Seq[Option[Int]])] = buildIfMatches(app)
    assert(task.isDefined)

    val (taskInfo, taskPorts) = task.get
    assertTcpHealthCheckProto(taskInfo, taskPorts.head.get)
  }

  test("Mesos TCP HealthCheck toMesos with Docker HOST networking and portIndex") {
    import MarathonTestHelper.Implicits._

    val app = MarathonTestHelper.makeBasicApp()
      .withDockerNetworks(HostNetwork)
      .withHealthCheck(mesosTcpHealthCheckWithPortIndex)

    val task: Option[(MesosProtos.TaskInfo, Seq[Option[Int]])] = buildIfMatches(app)
    assert(task.isDefined)

    val (taskInfo, taskPorts) = task.get
    assertTcpHealthCheckProto(taskInfo, taskPorts.head.get)
  }

  test("Mesos TCP HealthCheck toMesos with Docker HOST networking and port") {
    import MarathonTestHelper.Implicits._

    val app = MarathonTestHelper.makeBasicApp()
      .withDockerNetworks(HostNetwork)
      .withHealthCheck(mesosTcpHealthCheckWithPort)

    val task: Option[(MesosProtos.TaskInfo, Seq[Option[Int]])] = buildIfMatches(app)
    assert(task.isDefined)

    val (taskInfo, _) = task.get
    assertTcpHealthCheckProto(taskInfo, 80)
  }

  test("Mesos TCP HealthCheck toMesos with Docker BRIDGE networking and portIndex") {
    import MarathonTestHelper.Implicits._

    val app = MarathonTestHelper.makeBasicApp()
      .withNoPortDefinitions()
      .withDockerNetworks(BridgeNetwork())
      .withPortMappings(Seq(
        PortMapping(containerPort = 80, hostPort = Some(0), servicePort = 0, protocol = "tcp",
          name = Some("http"))
      ))
      .withHealthCheck(mesosTcpHealthCheckWithPortIndex)

    val task: Option[(MesosProtos.TaskInfo, Seq[Option[Int]])] = buildIfMatches(app)
    assert(task.isDefined)

    val (taskInfo, _) = task.get
    assertTcpHealthCheckProto(taskInfo, 80)
  }

  test("Mesos TCP HealthCheck toMesos with Docker BRIDGE networking and port") {
    import MarathonTestHelper.Implicits._

    val app = MarathonTestHelper.makeBasicApp()
      .withNoPortDefinitions()
      .withDockerNetworks(BridgeNetwork())
      .withPortMappings(Seq(
        PortMapping(containerPort = 8080, hostPort = Some(0), servicePort = 0, protocol = "tcp",
          name = Some("http"))
      ))
      .withHealthCheck(mesosTcpHealthCheckWithPort)

    val task: Option[(MesosProtos.TaskInfo, Seq[Option[Int]])] = buildIfMatches(app)
    assert(task.isDefined)

    val (taskInfo, _) = task.get
    assertTcpHealthCheckProto(taskInfo, 80)
  }

  test("Mesos TCP HealthCheck toMesos with Docker USER networking and a port mapping NOT requesting a host port, with portIndex") {
    import MarathonTestHelper.Implicits._

    val app = MarathonTestHelper.makeBasicApp()
      .withNoPortDefinitions()
      .withDockerNetworks(ContainerNetwork("whatever"))
      .withPortMappings(Seq(PortMapping(containerPort = 80, hostPort = None)))
      .withHealthCheck(mesosTcpHealthCheckWithPortIndex)

    val task: Option[(MesosProtos.TaskInfo, Seq[Option[Int]])] = buildIfMatches(app)
    assert(task.isDefined)

    val (taskInfo, _) = task.get
    assertTcpHealthCheckProto(taskInfo, 80)
  }

  test("Mesos TCP HealthCheck toMesos with Docker USER networking and a port mapping requesting a host port, with portIndex") {
    import MarathonTestHelper.Implicits._
    val app = MarathonTestHelper.makeBasicApp()
      .withNoPortDefinitions()
      .withDockerNetworks(ContainerNetwork("whatever"))
      .withPortMappings(Seq(PortMapping(containerPort = 80, hostPort = Some(0))))
      .withHealthCheck(mesosTcpHealthCheckWithPortIndex)

    val task: Option[(MesosProtos.TaskInfo, Seq[Option[Int]])] = buildIfMatches(app)
    assert(task.isDefined)

    val (taskInfo, _) = task.get
    assertTcpHealthCheckProto(taskInfo, 80)
  }

  test("Mesos TCP HealthCheck toMesos with Docker USER networking with port") {
    import MarathonTestHelper.Implicits._

    val app = MarathonTestHelper.makeBasicApp()
      .withNoPortDefinitions()
      .withDockerNetworks(ContainerNetwork("whatever"))
      .withPortMappings(Seq(PortMapping(containerPort = 31337, hostPort = Some(0))))
      .withHealthCheck(mesosTcpHealthCheckWithPort)

    val task: Option[(MesosProtos.TaskInfo, Seq[Option[Int]])] = buildIfMatches(app)
    assert(task.isDefined)

    val (taskInfo, _) = task.get
    assertTcpHealthCheckProto(taskInfo, 80)
  }

  test("Read Mesos TCP health check") {
    val portIndexJson =
      """
        {
          "protocol": "MESOS_TCP",
          "gracePeriodSeconds": 10,
          "intervalSeconds": 60,
          "timeoutSeconds": 20,
          "maxConsecutiveFailures": 0,
          "portIndex": 0,
          "delaySeconds": 15
        }
      """
    assert(fromJson(portIndexJson) == mesosTcpHealthCheckWithPortIndex)

    val portJson =
      """
        {
          "protocol": "MESOS_TCP",
          "gracePeriodSeconds": 10,
          "intervalSeconds": 60,
          "timeoutSeconds": 20,
          "maxConsecutiveFailures": 0,
          "port": 80,
          "delaySeconds": 15
        }
      """
    assert(fromJson(portJson) == mesosTcpHealthCheckWithPort)
  }

  test("Write Mesos TCP health check") {
    val portIndexJson =
      """
        {
          "protocol": "MESOS_TCP",
          "gracePeriodSeconds": 10,
          "intervalSeconds": 60,
          "timeoutSeconds": 20,
          "maxConsecutiveFailures": 0,
          "portIndex": 0,
          "delaySeconds": 15
        }
      """
    JsonTestHelper.assertThatJsonOf(mesosTcpHealthCheckWithPortIndex)
      .correspondsToJsonString(portIndexJson)

    val portJson =
      """
        {
          "protocol": "MESOS_TCP",
          "gracePeriodSeconds": 10,
          "intervalSeconds": 60,
          "timeoutSeconds": 20,
          "maxConsecutiveFailures": 0,
          "port": 80,
          "delaySeconds": 15
        }
      """
    JsonTestHelper.assertThatJsonOf(mesosTcpHealthCheckWithPort)
      .correspondsToJsonString(portJson)
  }

  def assertHealthCheckProto(healthCheckProto: MesosProtos.HealthCheck, protocol: MesosProtos.HealthCheck.Type): Unit = {
    assert(healthCheckProto.getType == protocol)
    assert(healthCheckProto.getGracePeriodSeconds == 10)
    assert(healthCheckProto.getIntervalSeconds == 60)
    assert(healthCheckProto.getConsecutiveFailures == 0)
  }

  def assertHttpHealthCheckProto(taskInfo: MesosProtos.TaskInfo, port: Int, scheme: String): Unit = {
    assert(taskInfo.hasHealthCheck)
    val healthCheckProto = taskInfo.getHealthCheck
    assertHealthCheckProto(healthCheckProto, MesosProtos.HealthCheck.Type.HTTP)

    assert(!healthCheckProto.hasTcp)
    assert(!healthCheckProto.hasCommand)
    assert(healthCheckProto.hasHttp)
    val httpProto = healthCheckProto.getHttp
    assert(httpProto.getPath == "/health")
    assert(httpProto.getScheme == scheme)
    assert(httpProto.getPort == port)
  }

  def assertTcpHealthCheckProto(taskInfo: MesosProtos.TaskInfo, port: Int): Unit = {
    assert(taskInfo.hasHealthCheck)
    val healthCheckProto = taskInfo.getHealthCheck
    assertHealthCheckProto(healthCheckProto, MesosProtos.HealthCheck.Type.TCP)

    assert(!healthCheckProto.hasCommand)
    assert(!healthCheckProto.hasHttp)
    assert(healthCheckProto.hasTcp)
    val tcpProto = healthCheckProto.getTcp
    assert(tcpProto.getPort == port)
  }

  def buildIfMatches(app: AppDefinition): Option[(MesosProtos.TaskInfo, Seq[Option[Int]])] = {
    val offer = MarathonTestHelper.makeBasicOfferWithRole(
      cpus = 1.0, mem = 128.0, disk = 1000.0, beginPort = 31000, endPort = 32000, role = ResourceRole.Unreserved).build

    val config = MarathonTestHelper.defaultConfig()
    val builder = new TaskBuilder(app, s => Task.Id(s.toString), config)
    val resourceMatch = RunSpecOfferMatcher.matchOffer(app, offer, Seq.empty, config.defaultAcceptedResourceRolesSet)
    resourceMatch match {
      case matches: ResourceMatchResponse.Match => Some(builder.build(offer, matches.resourceMatch, None))
      case _ => None
    }
  }

  val mesosHttpHealthCheckWithPortIndex = MesosHttpHealthCheck(
    path = Some("/health"),
    protocol = Protocol.MESOS_HTTP,
    portIndex = Some(PortReference(0)),
    gracePeriod = 10.seconds,
    interval = 60.seconds,
    maxConsecutiveFailures = 0)
  val mesosHttpHealthCheckWithPort = mesosHttpHealthCheckWithPortIndex.copy(portIndex = None, port = Some(80))

  val mesosTcpHealthCheckWithPortIndex = MesosTcpHealthCheck(
    portIndex = Some(PortReference(0)),
    gracePeriod = 10.seconds,
    interval = 60.seconds,
    maxConsecutiveFailures = 0)
  val mesosTcpHealthCheckWithPort = mesosTcpHealthCheckWithPortIndex.copy(portIndex = None, port = Some(80))

  private[this] def fromJson(json: String): HealthCheck = {
    val parsed = Json.parse(json)
    val appCheck: AppHealthCheck = parsed.as[AppHealthCheck]
    Raml.fromRaml(appCheck)
  }

  private[this] def shouldBeInvalid(hc: HealthCheck): Unit = {
    assert(validate(hc).isFailure)
  }

  private[this] def shouldBeValid(hc: HealthCheck): Unit = {
    val result = validate(hc)
    assert(result.isSuccess, s"violations: ${ValidationHelper.getAllRuleConstrains(result)}")
  }
}
