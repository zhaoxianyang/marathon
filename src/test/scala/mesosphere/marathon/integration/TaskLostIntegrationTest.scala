package mesosphere.marathon
package integration

import mesosphere.{ AkkaIntegrationFunTest, IntegrationTag, Unstable }
import mesosphere.marathon.Protos.Constraint.Operator
import mesosphere.marathon.UnstableTest
import mesosphere.marathon.api.v2.json.AppUpdate
import mesosphere.marathon.integration.facades.ITEnrichedTask
import mesosphere.marathon.integration.setup._

@IntegrationTest
@UnstableTest
class TaskLostIntegrationTest extends AkkaIntegrationFunTest with EmbeddedMarathonMesosClusterTest {
  override lazy val mesosNumMasters = 2
  override lazy val mesosNumSlaves = 2

  override val marathonArgs: Map[String, String] = Map(
    "reconciliation_initial_delay" -> "10000",
    "reconciliation_interval" -> "10000",
    "scale_apps_initial_delay" -> "1000",
    "scale_apps_interval" -> "5000",
    "min_revive_offers_interval" -> "100",
    "task_lost_expunge_gc" -> "30000",
    "task_lost_expunge_initial_delay" -> "1000",
    "task_lost_expunge_interval" -> "1000"
  )

  before {
    mesosCluster.agents(1).stop()
    mesosCluster.masters(1).stop()
    mesosCluster.masters.head.start()
    mesosCluster.agents.head.start()
    mesosCluster.waitForLeader().futureValue
  }

  after {
    // restoring the entire cluster increases the changes cleanUp will succeed.
    mesosCluster.masters.foreach(_.start())
    mesosCluster.agents.foreach(_.start())
    mesosCluster.waitForLeader().futureValue
    cleanUp()
  }

  test("A task lost with mesos master failover will not kill the task - https://github.com/mesosphere/marathon/issues/4214", Unstable, IntegrationTag) {
    Given("a new app")
    val app = appProxy(testBasePath / "app", "v1", instances = 1, healthCheck = None)
    waitForDeployment(marathon.createAppV2(app))
    val task = waitForTasks(app.id, 1).head

    When("We stop the slave, the task is declared lost")
    mesosCluster.agents.head.stop()
    waitForEventMatching("Task is declared lost") { matchEvent("TASK_UNREACHABLE", task) }

    And("The task is not removed from the task list")
    val lost = waitForTasks(app.id, 1).head
    lost.state should be("TASK_UNREACHABLE")

    When("We do a Mesos Master failover and start the slave again")
    mesosCluster.masters(1).start()
    mesosCluster.masters.head.stop()
    mesosCluster.agents.head.start()

    Then("The task reappears as running")
    waitForEventMatching("Task is declared running again") { matchEvent("TASK_RUNNING", task) }
  }

  test("A task lost with mesos master failover will start a replacement task", Unstable) {
    Given("a new app")
    val app = appProxy(testBasePath / "app", "v1", instances = 1, healthCheck = None)
    waitForDeployment(marathon.createAppV2(app))
    val task = waitForTasks(app.id, 1).head

    When("We stop the slave, the task is declared lost")
    mesosCluster.agents.head.stop()
    mesosCluster.agents(1).start()
    waitForEventMatching("Task is declared lost") { matchEvent("TASK_UNREACHABLE", task) }

    And("A replacement task is started")
    waitForEventWith("status_update_event", _.info("taskStatus") == "TASK_RUNNING")
    val tasks = marathon.tasks(app.id).value
    tasks should have size 2
    tasks.groupBy(_.state).keySet should be(Set("TASK_RUNNING", "TASK_UNREACHABLE"))
    val replacement = tasks.find(_.state == "TASK_RUNNING").get

    When("We do a Mesos Master failover and start the slave again")
    mesosCluster.masters(1).start()
    mesosCluster.masters.head.stop()
    mesosCluster.agents.head.start()

    Then("The task reappears as running and the replacement is killed")
    var isRunning = false
    var isKilled = false
    waitForEventMatching("Original task is running and replacement task is killed") { event =>
      isRunning |= matchEvent("TASK_RUNNING", task)(event)
      isKilled |= matchEvent("TASK_KILLED", replacement)(event)
      isRunning && isKilled
    }
    waitForTasks(app.id, 1).head should be(task)
  }

  // regression test for https://github.com/mesosphere/marathon/issues/4059
  test("Scaling down an app with constraints and lost tasks will succeed") {
    import mesosphere.marathon.Protos.Constraint
    Given("an app that is constrained to a unique hostname")
    val constraint: Constraint = Constraint.newBuilder
      .setField("hostname")
      .setOperator(Operator.UNIQUE)
      .setValue("")
      .build

    // start both slaves
    mesosCluster.agents.foreach(_.start())

    val app = appProxy(testBasePath / "app", "v1", instances = 2, healthCheck = None).copy(constraints = Set(constraint))

    waitForDeployment(marathon.createAppV2(app))
    val enrichedTasks = waitForTasks(app.id, 2)
    val task = enrichedTasks.find(t => t.host == "0").getOrElse(throw new RuntimeException("No matching task found on slave1"))

    When("agent1 is stopped")
    mesosCluster.agents.head.stop()
    Then("one task is declared lost")
    waitForEventMatching("Task is declared lost") { matchEvent("TASK_UNREACHABLE", task) }

    When("We try to scale down to one instance")
    marathon.updateApp(app.id, AppUpdate(instances = Some(1)))
    waitForEventMatching("deployment to scale down should be triggered") { matchDeploymentStart(app.id.toString) }

    Then("the deployment will eventually finish")
    waitForEventMatching("app should be scaled and deployment should be finished") { matchDeploymentSuccess(1, app.id.toString) }
    marathon.listDeploymentsForBaseGroup().value should have size 0
  }

  test("Scaling down an app with lost tasks will succeed") {
    val app = appProxy(testBasePath / "app", "v1", instances = 1, healthCheck = None)

    waitForDeployment(marathon.createAppV2(app))
    val enrichedTasks = waitForTasks(app.id, 1)
    val task = enrichedTasks.headOption.getOrElse(throw new RuntimeException("No matching task found"))

    When("agent1 is stopped")
    mesosCluster.agents.head.stop()
    Then("one task is declared lost")
    waitForEventMatching("Task is declared lost") { matchEvent("TASK_UNREACHABLE", task) }

    When("We try to scale down to one instance")
    marathon.updateApp(app.id, AppUpdate(instances = Some(0)))

    Then("the deployment will eventually finish")
    waitForEventMatching("app should be scaled and deployment should be finished") { matchDeploymentSuccess(1, app.id.toString) }
    marathon.listDeploymentsForBaseGroup().value should have size 0
  }

  test("A task lost with mesos master failover will expunge the task after gc timeout - https://github.com/mesosphere/marathon/issues/4212", Unstable, IntegrationTag) {
    Given("a new app")
    val app = appProxy(testBasePath / "app", "v1", instances = 1, healthCheck = None)
    waitForDeployment(marathon.createAppV2(app))
    val task = waitForTasks(app.id, 1).head

    When("We stop the slave, the task is declared lost")
    mesosCluster.agents.head.stop()
    mesosCluster.agents(1).start()
    waitForEventMatching("Task is declared lost") { matchEvent("TASK_LOST", task) }

    Then("The task is killed due to GC timeout and a replacement is started")
    val replacement = waitForTasks(app.id, 1).head
    replacement should not be task
  }

  def matchEvent(status: String, task: ITEnrichedTask): CallbackEvent => Boolean = { event =>
    event.info.get("taskStatus").contains(status) &&
      event.info.get("taskId").contains(task.id)
  }

  private def matchDeploymentSuccess(instanceCount: Int, appId: String): CallbackEvent => Boolean = { event =>
    val infoString = event.info.toString()
    event.eventType == "deployment_success" && infoString.contains(s"instances -> $instanceCount") && matchScaleApplication(infoString, appId)
  }

  private def matchDeploymentStart(appId: String): CallbackEvent => Boolean = { event =>
    val infoString = event.info.toString()
    event.eventType == "deployment_info" && matchScaleApplication(infoString, appId)
  }

  private def matchScaleApplication(infoString: String, appId: String): Boolean = {
    infoString.contains(s"List(Map(actions -> List(Map(action -> ScaleApplication, app -> $appId)))))")
  }

}
