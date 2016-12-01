package mesosphere.marathon
package integration

import mesosphere.{ AkkaIntegrationFunTest, EnvironmentFunTest, Unstable }
import mesosphere.marathon.integration.facades.MarathonFacade._
import mesosphere.marathon.integration.setup.{ EmbeddedMarathonTest, MesosConfig }
import mesosphere.marathon.raml.{ App, Container, DockerContainer, EngineType }
import mesosphere.marathon.state.PathId._

import scala.concurrent.duration._

@IntegrationTest
class DockerAppIntegrationTest
    extends AkkaIntegrationFunTest
    with EmbeddedMarathonTest
    with EnvironmentFunTest {

  override lazy val mesosConfig = MesosConfig(containerizers = "docker,mesos")

  // FIXME (gkleiman): Docker tests don't work under Docker Machine yet. So they can be disabled through an env variable.
  override val envVar = "RUN_DOCKER_INTEGRATION_TESTS"

  //clean up state before running the test case
  after(cleanUp())

  test("deploy a simple Docker app") {
    Given("a new Docker app")
    val app = App(
      id = (testBasePath / "dockerapp").toString,
      cmd = Some("sleep 600"),
      container = Some(Container(`type` = EngineType.Docker, docker = Some(DockerContainer(image = "busybox")))),
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

  test("create a simple docker app using http health checks with HOST networking", Unstable) {
    Given("a new app")
    val app = dockerAppProxy(testBasePath / "docker-http-app", "v1", instances = 1, healthCheck = Some(appProxyHealthCheck()))
    val check = appProxyCheck(app.id.toPath, "v1", state = true)

    When("The app is deployed")
    val result = marathon.createAppV2(app)

    Then("The app is created")
    result.code should be(201) //Created
    extractDeploymentIds(result) should have size 1
    waitForDeployment(result)
    check.pingSince(5.seconds) should be(true) //make sure, the app has really started
  }
}
