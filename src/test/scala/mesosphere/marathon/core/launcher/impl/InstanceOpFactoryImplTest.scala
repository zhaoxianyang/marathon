package mesosphere.marathon.core.launcher.impl

import mesosphere.marathon.test.MarathonSpec
import mesosphere.marathon.core.base.{ Clock, ConstantClock }
import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.Instance.AgentInfo
import mesosphere.marathon.core.pod.{ MesosContainer, PodDefinition }
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.raml.{ Endpoint, Resources }
import mesosphere.marathon.state.PathId
import org.scalatest.Matchers

import scala.collection.immutable.Seq

class InstanceOpFactoryImplTest extends MarathonSpec with Matchers {

  import InstanceOpFactoryImplTest._

  test("minimal ephemeralPodInstance") {
    val pod = minimalPod
    val tc = TestCase(pod, agentInfo)
    implicit val clock = ConstantClock()
    val instance = InstanceOpFactoryImpl.ephemeralPodInstance(pod, agentInfo, tc.taskIDs, tc.hostPortsAllocatedFromOffer, tc.instanceId)
    check(tc, instance)
  }

  test("ephemeralPodInstance with endpoint no host port") {
    val pod = minimalPod.copy(containers = minimalPod.containers.map { ct =>
      ct.copy(endpoints = Seq(Endpoint(name = "web")))
    })
    val tc = TestCase(pod, agentInfo)
    implicit val clock = ConstantClock()
    val instance = InstanceOpFactoryImpl.ephemeralPodInstance(pod, agentInfo, tc.taskIDs, tc.hostPortsAllocatedFromOffer, tc.instanceId)
    check(tc, instance)
  }

  test("ephemeralPodInstance with endpoint one host port") {
    val pod = minimalPod.copy(containers = minimalPod.containers.map { ct =>
      ct.copy(endpoints = Seq(Endpoint(name = "web", hostPort = Some(80))))
    })
    val tc = TestCase(pod, agentInfo)
    implicit val clock = ConstantClock()
    val instance = InstanceOpFactoryImpl.ephemeralPodInstance(pod, agentInfo, tc.taskIDs, tc.hostPortsAllocatedFromOffer, tc.instanceId)
    check(tc, instance)
  }

  test("ephemeralPodInstance with multiple endpoints, mixed host ports") {
    val pod = minimalPod.copy(containers = minimalPod.containers.map { ct =>
      ct.copy(endpoints = Seq(
        Endpoint(name = "ep0", hostPort = Some(80)),
        Endpoint(name = "ep1"),
        Endpoint(name = "ep2", hostPort = Some(90)),
        Endpoint(name = "ep3")
      ))
    })
    val tc = TestCase(pod, agentInfo)
    implicit val clock = ConstantClock()
    val instance = InstanceOpFactoryImpl.ephemeralPodInstance(pod, agentInfo, tc.taskIDs, tc.hostPortsAllocatedFromOffer, tc.instanceId)
    check(tc, instance)
  }

  test("ephemeralPodInstance with multiple containers, multiple endpoints, mixed host ports") {
    val pod = minimalPod.copy(containers = Seq(
      MesosContainer(name = "ct0", resources = someRes, endpoints = Seq(Endpoint(name = "ep0"))),
      MesosContainer(name = "ct1", resources = someRes, endpoints = Seq(
        Endpoint(name = "ep1", hostPort = Some(1)),
        Endpoint(name = "ep2", hostPort = Some(2))
      )),
      MesosContainer(name = "ct2", resources = someRes, endpoints = Seq(Endpoint(name = "ep3", hostPort = Some(3))))
    ))
    val tc = TestCase(pod, agentInfo)
    implicit val clock = ConstantClock()
    val instance = InstanceOpFactoryImpl.ephemeralPodInstance(pod, agentInfo, tc.taskIDs, tc.hostPortsAllocatedFromOffer, tc.instanceId)
    check(tc, instance)
  }

  test("ephemeralPodInstance with multiple containers, multiple endpoints, mixed allocated/unallocated host ports") {
    val pod = minimalPod.copy(containers = Seq(
      MesosContainer(name = "ct0", resources = someRes, endpoints = Seq(Endpoint(name = "ep0"))),
      MesosContainer(name = "ct1", resources = someRes, endpoints = Seq(
        Endpoint(name = "ep1", hostPort = Some(1)),
        Endpoint(name = "ep2", hostPort = Some(0))
      )),
      MesosContainer(name = "ct2", resources = someRes, endpoints = Seq(Endpoint(name = "ep3", hostPort = Some(3))))
    ))
    val tc = TestCase(pod, agentInfo)
    implicit val clock = ConstantClock()
    val instance = InstanceOpFactoryImpl.ephemeralPodInstance(
      pod, agentInfo, tc.taskIDs, tc.hostPortsAllocatedFromOffer, tc.instanceId)
    check(tc, instance)
  }

  def check(tc: TestCase, instance: Instance)(implicit clock: Clock): Unit = {
    import tc._

    instance.instanceId should be(instanceId)
    instance.agentInfo should be(agentInfo)
    instance.tasksMap.size should be(pod.containers.size)
    instance.tasksMap.keys.toSeq should be(taskIDs)

    val mappedPorts: Seq[Int] = instance.tasksMap.values.view.flatMap(_.launched.map(_.hostPorts)).flatten.toIndexedSeq
    mappedPorts should be(hostPortsAllocatedFromOffer.flatten)

    val expectedHostPortsPerCT: Map[String, Seq[Int]] = pod.containers.map { ct =>
      ct.name -> ct.endpoints.flatMap{ ep =>
        ep.hostPort match {
          case Some(hostPort) if hostPort == 0 => Some(fakeAllocatedPort)

          // isn't there a more compact way to represent the next two lines?
          case Some(hostPort) => Some(hostPort)
          case None => None
        }
      }
    }(collection.breakOut)

    val allocatedPortsPerTask: Map[String, Seq[Int]] = instance.tasksMap.map {
      case (taskId, task) =>
        val ctName = taskId.containerName.get
        val ports: Seq[Int] = task.launched.map(_.hostPorts).getOrElse(Seq.empty[Int])
        ctName -> ports
    }(collection.breakOut)

    allocatedPortsPerTask should be(expectedHostPortsPerCT)

    instance.tasksMap.foreach {
      case (_, task) =>
        task.status.stagedAt should be(clock.now())
        task.status.condition should be(Condition.Created)
    }
  }
}

object InstanceOpFactoryImplTest {

  val someRes = Resources(1, 128, 0, 0)

  val fakeAllocatedPort = 99

  val minimalPod = PodDefinition(
    id = PathId("/foo"),
    containers = Seq(MesosContainer(name = "ct1", resources = someRes))
  )

  val agentInfo = AgentInfo("agent1", None, Seq.empty)

  case class TestCase(pod: PodDefinition, agentInfo: AgentInfo) {
    val instanceId: Instance.Id = Instance.Id.forRunSpec(pod.id)

    val taskIDs: Seq[Task.Id] = pod.containers.map { ct =>
      Task.Id.forInstanceId(instanceId, Some(ct))
    }(collection.breakOut)

    // faking it: we always get the host port that we try to allocate
    val hostPortsAllocatedFromOffer: Seq[Option[Int]] = pod.containers.flatMap(_.endpoints.map(_.hostPort)).map {
      case Some(port) if port == 0 => Some(fakeAllocatedPort)

      // again, isn't there more compact way to do this?
      case Some(port) => Some(port)
      case None => None
    }
  }
}
