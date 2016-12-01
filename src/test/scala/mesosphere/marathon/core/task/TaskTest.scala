package mesosphere.marathon
package core.task

import mesosphere.marathon.core.base.ConstantClock
import mesosphere.marathon.core.instance.{ Instance, TestTaskBuilder }
import mesosphere.marathon.core.pod.{ ContainerNetwork, HostNetwork }
import mesosphere.marathon.core.task.Task.LocalVolumeId
import mesosphere.marathon.core.task.bus.MesosTaskStatusTestHelper
import mesosphere.marathon.core.task.update.{ TaskUpdateEffect, TaskUpdateOperation }
import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.state.{ AppDefinition, PathId }
import mesosphere.marathon.stream._
import mesosphere.marathon.test.{ MarathonTestHelper, Mockito }
import org.apache.mesos.{ Protos => MesosProtos }
import org.scalatest.OptionValues._
import org.scalatest.{ FunSuite, GivenWhenThen, Matchers }

import scala.concurrent.duration._

class TaskTest extends FunSuite with Mockito with GivenWhenThen with Matchers {

  class Fixture {

    val clock = ConstantClock()

    val appWithoutIpAddress = AppDefinition(id = PathId("/foo/bar"), networks = Seq(HostNetwork))
    val appVirtualNetworks = Seq(ContainerNetwork("whatever"))
    val appWithIpAddress = AppDefinition(
      id = PathId("/foo/bar"),
      portDefinitions = Seq.empty,
      networks = appVirtualNetworks
    )

    val networkWithoutIp = MesosProtos.NetworkInfo.newBuilder.build()

    val ipString1 = "123.123.123.123"
    val ipAddress1 = MesosProtos.NetworkInfo.IPAddress.newBuilder().setIpAddress(ipString1).build()

    val ipString2 = "123.123.123.124"
    val ipAddress2 = MesosProtos.NetworkInfo.IPAddress.newBuilder().setIpAddress(ipString2).build()

    val networkWithOneIp1 = MesosProtos.NetworkInfo.newBuilder.addIpAddresses(ipAddress1).build()
    val networkWithOneIp2 = MesosProtos.NetworkInfo.newBuilder.addIpAddresses(ipAddress2).build()

    val networkWithMultipleIps = MesosProtos.NetworkInfo.newBuilder.addAllIpAddresses(Seq(ipAddress1, ipAddress2)).build()

    val host: String = "agent1.mesos"

    import MarathonTestHelper.Implicits._

    val taskWithoutIp =
      TestTaskBuilder.Helper
        .runningTaskForApp(appWithoutIpAddress.id)
        .withAgentInfo(_.copy(host = host))

    val taskWithOneIp =
      TestTaskBuilder.Helper
        .runningTaskForApp(appWithoutIpAddress.id)
        .withAgentInfo(_.copy(host = host))
        .withNetworkInfos(Seq(networkWithOneIp1))

    val taskWithMultipleNetworksAndOneIp =
      TestTaskBuilder.Helper
        .runningTaskForApp(appWithoutIpAddress.id)
        .withAgentInfo(_.copy(host = host))
        .withNetworkInfos(Seq(networkWithoutIp, networkWithOneIp1))

    val taskWithMultipleNetworkAndNoIp =
      TestTaskBuilder.Helper
        .runningTaskForApp(appWithoutIpAddress.id)
        .withAgentInfo(_.copy(host = host))
        .withNetworkInfos(Seq(networkWithoutIp, networkWithoutIp))

    val taskWithOneNetworkAndMultipleIPs =
      TestTaskBuilder.Helper
        .runningTaskForApp(appWithoutIpAddress.id)
        .withAgentInfo(_.copy(host = host))
        .withNetworkInfos(Seq(networkWithMultipleIps))

    val taskWithMultipleNetworkAndMultipleIPs =
      TestTaskBuilder.Helper
        .runningTaskForApp(appWithoutIpAddress.id)
        .withAgentInfo(_.copy(host = host))
        .withNetworkInfos(Seq(networkWithOneIp1, networkWithOneIp2))
  }

  test("effectiveIpAddress returns the container ip for MarathonTask instances with one NetworkInfo (if the app requests an IP)") {
    val f = new Fixture
    f.taskWithOneIp.effectiveIpAddress(f.appWithIpAddress).value should equal(f.ipString1)
  }

  test("effectiveIpAddress returns the first container ip for for MarathonTask instances with multiple NetworkInfos (if the app requests an IP)") {
    val f = new Fixture
    f.taskWithMultipleNetworksAndOneIp.effectiveIpAddress(f.appWithIpAddress).value should equal (f.ipString1)
  }

  test("effectiveIpAddress returns None if there is no ip") {
    val f = new Fixture
    f.taskWithMultipleNetworkAndNoIp.effectiveIpAddress(f.appWithIpAddress) should be (None)
  }

  test("effectiveIpAddress returns the agent ip for MarathonTask instances with one NetworkInfo (if the app does NOT request an IP)") {
    val f = new Fixture
    f.taskWithOneIp.effectiveIpAddress(f.appWithoutIpAddress).value should equal(f.host)
  }

  test("ipAddresses returns None for MarathonTask instances with no IPs") {
    val f = new Fixture
    f.taskWithoutIp.status.ipAddresses should be (None)
  }

  test("ipAddresses returns an empty list for MarathonTask instances with no IPs and multiple NetworkInfos") {
    val f = new Fixture
    f.taskWithMultipleNetworkAndNoIp.status.ipAddresses.value should be (empty)
  }

  test("ipAddresses returns all IPs for MarathonTask instances with multiple IPs") {
    val f = new Fixture
    f.taskWithMultipleNetworkAndMultipleIPs.status.ipAddresses.value should equal(Seq(f.ipAddress1, f.ipAddress2))
  }

  test("ipAddresses returns all IPs for MarathonTask instances with multiple IPs and multiple NetworkInfos") {
    val f = new Fixture
    f.taskWithMultipleNetworkAndMultipleIPs.status.ipAddresses.value should equal(Seq(f.ipAddress1, f.ipAddress2))
  }

  test("ipAddresses returns one IP for MarathonTask instances with one IP and one NetworkInfo") {
    val f = new Fixture
    f.taskWithOneIp.status.ipAddresses.value should equal(Seq(f.ipAddress1))
  }

  test("ipAddresses returns one IP for MarathonTask instances with one IP and multiple NetworkInfo") {
    val f = new Fixture
    f.taskWithMultipleNetworksAndOneIp.status.ipAddresses.value should equal(Seq(f.ipAddress1))
  }

  test("VolumeId should be parsable, even if the task contains a dot in the appId") {
    val volumeIdString = "registry.domain#storage#8e1f0af7-3fdd-11e6-a2ab-2687a99fcff1"
    val volumeId = LocalVolumeId.unapply(volumeIdString)
    volumeId should not be None
    volumeId should be (Some(LocalVolumeId(PathId.fromSafePath("registry.domain"), "storage", "8e1f0af7-3fdd-11e6-a2ab-2687a99fcff1")))
  }

  test("isUnreachableExpired returns if task is inactive") {
    val f = new Fixture

    val condition = Condition.Unreachable
    val taskId = Task.Id.forRunSpec(f.appWithIpAddress.id)
    val mesosStatus = MesosTaskStatusTestHelper.mesosStatus(condition, taskId, f.clock.now - 5.minutes)
    val task = TestTaskBuilder.Helper.minimalTask(taskId, f.clock.now - 5.minutes, mesosStatus, condition)

    task.isUnreachableExpired(f.clock.now, 4.minutes) should be(true)
    task.isUnreachableExpired(f.clock.now, 10.minutes) should be(false)
  }

  test("a reserved task throws an exception on MesosUpdate") {
    val f = new Fixture

    val condition = Condition.Reserved
    val taskId = Task.Id.forRunSpec(f.appWithIpAddress.id)
    val agentInfo = mock[Instance.AgentInfo]
    val reservation = mock[Task.Reservation]
    val status = Task.Status(f.clock.now, None, None, condition)
    val task = Task.Reserved(taskId, agentInfo, reservation, status, f.clock.now)

    val mesosStatus = MesosTaskStatusTestHelper.running(taskId)
    val op = TaskUpdateOperation.MesosUpdate(Condition.Running, mesosStatus, f.clock.now)

    val effect = task.update(op)

    effect shouldBe a[TaskUpdateEffect.Failure]
  }

  test("a reserved task returns an update") {
    val f = new Fixture

    val condition = Condition.Reserved
    val taskId = Task.Id.forRunSpec(f.appWithIpAddress.id)
    val agentInfo = mock[Instance.AgentInfo]
    val reservation = mock[Task.Reservation]
    val status = Task.Status(f.clock.now, None, None, condition)
    val task = Task.Reserved(taskId, agentInfo, reservation, status, f.clock.now)

    val op = TaskUpdateOperation.LaunchOnReservation(f.clock.now, status, Seq.empty)

    val effect = task.update(op)

    effect shouldBe a[TaskUpdateEffect.Update]
  }

  test("Task.Id as key in Map") {
    val taskId1 = Task.Id("foobar")
    val taskId2 = Task.Id("baz")

    val m = Map(taskId1 -> 1)

    m.get(taskId1) should be('defined)
    m.get(taskId2) should not be 'defined
    m(taskId1) should be(1)
    an[NoSuchElementException] should be thrownBy m(taskId2)
  }

}
