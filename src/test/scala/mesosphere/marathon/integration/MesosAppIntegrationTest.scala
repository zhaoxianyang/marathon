package mesosphere.marathon
package integration

import mesosphere.Unstable
import mesosphere.{ AkkaIntegrationFunTest, EnvironmentFunTest }
import mesosphere.marathon.core.health.{ MesosHttpHealthCheck, PortReference }
import mesosphere.marathon.core.pod.{ HostNetwork, HostVolume, MesosContainer, PodDefinition }
import mesosphere.marathon.integration.facades.MarathonFacade._
import mesosphere.marathon.integration.setup.{ EmbeddedMarathonTest, MesosConfig, WaitTestSupport }
import mesosphere.marathon.raml.{ App, Container, DockerContainer, EngineType }
import mesosphere.marathon.state.PathId
import mesosphere.marathon.state.PathId._

import scala.collection.immutable.Seq
import scala.concurrent.duration._

@IntegrationTest
class MesosAppIntegrationTest
    extends AkkaIntegrationFunTest
    with EmbeddedMarathonTest
    with EnvironmentFunTest {

  // Integration tests using docker image provisioning with the Mesos containerizer need to be
  // run as root in a Linux environment. They have to be explicitly enabled through an env variable.
  override val envVar = "RUN_MESOS_INTEGRATION_TESTS"
  // Configure Mesos to provide the Mesos containerizer with Docker image support.
  override lazy val mesosConfig = MesosConfig(
    launcher = "linux",
    containerizers = "mesos",
    isolation = Some("filesystem/linux,docker/runtime"),
    imageProviders = Some("docker"))

  private[this] def simplePod(podId: PathId): PodDefinition = PodDefinition(
    id = podId,
    containers = Seq(
      MesosContainer(
        name = "task1",
        exec = Some(raml.MesosExec(raml.ShellCommand("sleep 1000"))),
        resources = raml.Resources(cpus = 0.1, mem = 32.0)
      )
    ),
    networks = Seq(HostNetwork),
    instances = 1
  )

  //clean up state before running the test case
  before(cleanUp())

  test("deploy a simple Docker app using the Mesos containerizer") {
    Given("a new Docker app")
    val app = App(
      id = (testBasePath / "mesosdockerapp").toString,
      cmd = Some("sleep 600"),
      container = Some(Container(`type` = EngineType.Mesos, docker = Some(DockerContainer(image = "busybox")))),
      cpus = 0.2,
      mem = 16.0,
      instances = 1
    )

    When("The app is deployed")
    val result = marathon.createAppV2(app)

    Then("The app is created")
    result.code should be(201) // Created
    extractDeploymentIds(result) should have size 1
    waitForDeployment(result)
    waitForTasks(app.id.toPath, 1) // The app has really started
  }

  test("deploy a simple pod", Unstable) {
    Given("a pod with a single task")
    val pod = simplePod(testBasePath / "simplepod")

    When("The pod is deployed")
    val createResult = marathon.createPodV2(pod)

    Then("The pod is created")
    createResult.code should be(201) // Created
    waitForDeployment(createResult)
    waitForPod(pod.id)

    When("The pod should be scaled")
    val scaledPod = pod.copy(instances = 2)
    val updateResult = marathon.updatePod(pod.id, scaledPod)

    Then("The pod is scaled")
    updateResult.code should be(200)
    waitForDeployment(updateResult)

    When("The pod should be deleted")
    val deleteResult = marathon.deletePod(pod.id)

    Then("The pod is deleted")
    deleteResult.code should be (202) // Deleted
    waitForDeployment(deleteResult)
  }

  test("deploy a simple pod with health checks", Unstable) {
    val projectDir = sys.props.getOrElse("user.dir", ".")
    val homeDir = sys.props.getOrElse("user.home", "~")

    Given("a pod with two tasks that are health checked")
    val podId = testBasePath / "healthypod"
    val containerDir = "/opt/marathon"
    def appMockCommand(port: String) = appProxyCommand(podId, "v1", containerDir, port)

    val pod = PodDefinition(
      id = podId,
      containers = Seq(
        MesosContainer(
          name = "task1",
          exec = Some(raml.MesosExec(raml.ShellCommand(appMockCommand("$ENDPOINT_TASK1")))),
          resources = raml.Resources(cpus = 0.1, mem = 32.0),
          endpoints = Seq(raml.Endpoint(name = "task1", hostPort = Some(0))),
          image = Some(raml.Image(raml.ImageType.Docker, "openjdk:8-jre-alpine")),
          healthCheck = Some(MesosHttpHealthCheck(portIndex = Some(PortReference("task1")), path = Some("/"))),
          volumeMounts = Seq(
            raml.VolumeMount("target", s"$containerDir/target", Some(true)),
            raml.VolumeMount("ivy2", s"$containerDir/.ivy2", Some(true)),
            raml.VolumeMount("sbt", s"$containerDir/.sbt", Some(true))
          )
        ),
        MesosContainer(
          name = "task2",
          exec = Some(raml.MesosExec(raml.ShellCommand(appMockCommand("$ENDPOINT_TASK2")))),
          resources = raml.Resources(cpus = 0.1, mem = 32.0),
          endpoints = Seq(raml.Endpoint(name = "task2", hostPort = Some(0))),
          image = Some(raml.Image(raml.ImageType.Docker, "openjdk:8-jre-alpine")),
          healthCheck = Some(MesosHttpHealthCheck(portIndex = Some(PortReference("task2")), path = Some("/"))),
          volumeMounts = Seq(
            raml.VolumeMount("target", s"$containerDir/target", Some(true)),
            raml.VolumeMount("ivy2", s"$containerDir/.ivy2", Some(true)),
            raml.VolumeMount("sbt", s"$containerDir/.sbt", Some(true))
          )
        )
      ),
      podVolumes = Seq(
        HostVolume("target", s"$projectDir/target"),
        HostVolume("ivy2", s"$homeDir/.ivy2"),
        HostVolume("sbt", s"$homeDir/.sbt")
      ),
      networks = Seq(HostNetwork),
      instances = 1
    )

    val check = appProxyCheck(pod.id, "v1", state = true)

    When("The pod is deployed")
    val createResult = marathon.createPodV2(pod)

    Then("The pod is created")
    createResult.code should be(201) //Created
    // The timeout is 5 minutes because downloading and provisioning the Python image can take some time.
    waitForDeployment(createResult, 300.seconds)
    waitForPod(podId)
    check.pingSince(20.seconds) should be(true) //make sure, the app has really started

    When("The pod definition is changed")
    val updatedPod = pod.copy(
      containers = pod.containers :+ MesosContainer(
      name = "task3",
      exec = Some(raml.MesosExec(raml.ShellCommand("sleep 1000"))),
      resources = raml.Resources(cpus = 0.1, mem = 32.0)
    )
    )
    val updateResult = marathon.updatePod(pod.id, updatedPod)

    Then("The pod is updated")
    updateResult.code should be(200)
    waitForDeployment(updateResult)

    When("The pod should be deleted")
    val deleteResult = marathon.deletePod(pod.id)

    Then("The pod is deleted")
    deleteResult.code should be (202) // Deleted
    waitForDeployment(deleteResult)
  }

  test("deleting a group deletes pods deployed in the group") {
    Given("a deployed pod")
    val pod = simplePod(testBasePath / "simplepod")
    val createResult = marathon.createPodV2(pod)
    createResult.code should be (201) //Created
    waitForDeployment(createResult)
    waitForPod(pod.id)
    marathon.listPodsInBaseGroup.value should have size 1

    Then("The pod should show up as a group")
    val groupResult = marathon.group(testBasePath)
    groupResult.code should be (200)

    When("The pod group is deleted")
    val deleteResult = marathon.deleteGroup(testBasePath)
    deleteResult.code should be (200)
    waitForDeployment(deleteResult)

    Then("The pod is deleted")
    marathon.listPodsInBaseGroup.value should have size 0
  }

  test("list pod versions") {
    Given("a new pod")
    val pod = simplePod(testBasePath / "simplepod")
    val createResult = marathon.createPodV2(pod)
    createResult.code should be (201) //Created
    waitForDeployment(createResult)
    waitForPod(pod.id)

    When("The list of versions is fetched")
    val podVersions = marathon.listPodVersions(pod.id)

    Then("The response should contain all the versions")
    podVersions.code should be (200)
    podVersions.value should have size 1
    podVersions.value.head should be (createResult.value.version)
  }

  test("correctly version pods", Unstable) {
    Given("a new pod")
    val pod = simplePod(testBasePath / "simplepod")
    val createResult = marathon.createPodV2(pod)
    createResult.code should be (201) //Created
    val originalVersion = createResult.value.version
    waitForDeployment(createResult)
    waitForPod(pod.id)

    When("A task is added to the pod")
    val updatedPod = pod.copy(
      containers = pod.containers :+ MesosContainer(
      name = "task3",
      exec = Some(raml.MesosExec(raml.ShellCommand("sleep 1000"))),
      resources = raml.Resources(cpus = 0.1, mem = 32.0)
    )
    )
    val updateResult = marathon.updatePod(pod.id, updatedPod)
    updateResult.code should be (200)
    val updatedVersion = updateResult.value.version
    waitForDeployment(updateResult)

    Then("It should create a new version with the right data")
    val originalVersionResult = marathon.podVersion(pod.id, originalVersion)
    originalVersionResult.code should be (200)
    originalVersionResult.value.containers should have size 1

    val updatedVersionResult = marathon.podVersion(pod.id, updatedVersion)
    updatedVersionResult.code should be (200)
    updatedVersionResult.value.containers should have size 2
  }

  test("stop (forcefully delete) a pod deployment") {
    Given("a pod with constraints that cannot be fulfilled")
    val constraint = Protos.Constraint.newBuilder().setField("nonExistent").setOperator(Protos.Constraint.Operator.CLUSTER).setValue("na").build()
    val pod = simplePod(testBasePath / "simplepod").copy(
      constraints = Set(constraint)
    )

    val createResult = marathon.createPodV2(pod)
    createResult.code should be (201) //Created
    val deploymentId = createResult.originalResponse.headers.find(_.name == "Marathon-Deployment-Id").map(_.value)
    deploymentId shouldBe defined

    Then("the deployment gets created")
    WaitTestSupport.validFor("deployment visible", 5.second)(marathon.listDeploymentsForBaseGroup().value.size == 1)

    When("the deployment is deleted")
    val deleteResult = marathon.deleteDeployment(deploymentId.get, force = true)
    deleteResult.code should be (202)

    Then("the deployment should be gone")
    waitForEvent("deployment_failed")
    WaitTestSupport.waitUntil("Deployments get removed from the queue", 30.seconds) {
      marathon.listDeploymentsForBaseGroup().value.isEmpty
    }

    Then("the pod should still be there")
    marathon.pod(pod.id).code should be (200)
  }

  test("rollback a pod deployment") {
    Given("a pod with constraints that cannot be fulfilled")
    val constraint = Protos.Constraint.newBuilder().setField("nonExistent").setOperator(Protos.Constraint.Operator.CLUSTER).setValue("na").build()
    val pod = simplePod(testBasePath / "simplepod").copy(
      constraints = Set(constraint)
    )

    val createResult = marathon.createPodV2(pod)
    createResult.code should be (201) //Created
    val deploymentId = createResult.originalResponse.headers.find(_.name == "Marathon-Deployment-Id").map(_.value)
    deploymentId shouldBe defined

    Then("the deployment gets created")
    WaitTestSupport.validFor("deployment visible", 5.second)(marathon.listDeploymentsForBaseGroup().value.size == 1)

    When("the deployment is rolled back")
    val deleteResult = marathon.deleteDeployment(deploymentId.get, force = false)
    deleteResult.code should be (200)

    Then("the deployment should be gone")
    waitForEvent("deployment_failed") // ScalePod
    waitForDeployment(deleteResult) // StopPod
    WaitTestSupport.waitUntil("Deployments get removed from the queue", 30.seconds) {
      marathon.listDeploymentsForBaseGroup().value.isEmpty
    }

    Then("the pod should also be gone")
    marathon.pod(pod.id).code should be (404)
  }

  test("delete pod instances") {
    Given("a new pod with 2 instances")
    val pod = simplePod(testBasePath / "simplepod").copy(
      instances = 3
    )

    When("The pod is created")
    val createResult = marathon.createPodV2(pod)
    createResult.code should be (201) //Created
    waitForDeployment(createResult)
    waitForPod(pod.id)

    Then("Three instances should be running")
    val status1 = marathon.status(pod.id)
    status1.code should be (200)
    status1.value.instances should have size 3

    When("An instance is deleted")
    val instanceId = status1.value.instances.head.id
    val deleteResult1 = marathon.deleteInstance(pod.id, instanceId)
    deleteResult1.code should be (200)

    Then("The deleted instance should be restarted")
    waitForEventWith("status_update_event", _.info("taskStatus") == "TASK_KILLED")
    waitForEventWith("status_update_event", _.info("taskStatus") == "TASK_RUNNING")
    val status2 = marathon.status(pod.id)
    status2.code should be (200)
    status2.value.instances should have size 3
  }
}
