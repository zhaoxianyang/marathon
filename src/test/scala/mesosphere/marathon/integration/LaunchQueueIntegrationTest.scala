package mesosphere.marathon
package integration

import mesosphere.AkkaIntegrationFunTest
import mesosphere.marathon.integration.setup._
import mesosphere.marathon.raml.App

import scala.concurrent.duration._

@IntegrationTest
class LaunchQueueIntegrationTest extends AkkaIntegrationFunTest with EmbeddedMarathonTest {
  test("GET /v2/queue with an empty queue") {
    Given("no pending deployments")
    marathon.listDeploymentsForBaseGroup().value should have size 0

    Then("the launch queue should be empty")
    val response = marathon.launchQueue()
    response.code should be (200)

    val queue = response.value.queue
    queue should have size 0
  }

  test("GET /v2/queue with pending app") {
    Given("a new app with constraints that cannot be fulfilled")
    val c = Seq("nonExistent", "CLUSTER", "na")
    val appId = testBasePath / "app"
    val app = App(appId.toString, constraints = Seq(c), cmd = Some("na"), instances = 5, portDefinitions = None)
    val create = marathon.createAppV2(app)
    create.code should be (201) // Created

    Then("the app shows up in the launch queue")
    WaitTestSupport.waitUntil("Deployment is put in the deployment queue", 30.seconds) { marathon.launchQueue().value.queue.size == 1 }
    val response = marathon.launchQueue()
    response.code should be (200)

    val queue = response.value.queue
    queue should have size 1
    queue.head.app.id should be (appId)
    queue.head.count should be (5)
  }
}
