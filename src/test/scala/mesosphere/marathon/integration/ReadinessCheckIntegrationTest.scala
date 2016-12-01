package mesosphere.marathon
package integration

import java.io.File

import mesosphere.{ AkkaIntegrationFunTest, Unstable }
import mesosphere.marathon.api.v2.json.AppUpdate
import mesosphere.marathon.core.health.{ HealthCheck, MarathonHttpHealthCheck, PortReference }
import mesosphere.marathon.core.readiness.ReadinessCheck
import mesosphere.marathon.integration.setup._
import mesosphere.marathon.raml.Resources
import mesosphere.marathon.state._
import org.apache.commons.io.FileUtils
import org.scalatest.concurrent.Eventually

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.util.Try

@IntegrationTest
class ReadinessCheckIntegrationTest extends AkkaIntegrationFunTest with EmbeddedMarathonTest with Eventually {

  //clean up state before running the test case
  before(cleanUp())

  test("A deployment of an application with readiness checks (no health) does finish when the plan is ready") {
    deploy(serviceProxy("/readynohealth".toTestPath, "phase(block1!,block2!,block3!)", withHealth = false), continue = true)
  }

  test("A deployment of an application with readiness checks and health does finish when health checks succeed and plan is ready") {
    deploy(serviceProxy("/readyhealth".toTestPath, "phase(block1!,block2!,block3!)", withHealth = true), continue = true)
  }

  test("A deployment of an application without readiness checks and health does finish when health checks succeed") {
    deploy(serviceProxy("/noreadyhealth".toTestPath, "phase()", withHealth = true), continue = false)
  }

  test("A deployment of an application without readiness checks and without health does finish") {
    deploy(serviceProxy("/noreadynohealth".toTestPath, "phase()", withHealth = false), continue = false)
  }

  test("An upgrade of an application will wait for the readiness checks", Unstable) {
    val serviceDef = serviceProxy("/upgrade".toTestPath, "phase(block1!,block2!,block3!)", withHealth = false)
    deploy(serviceDef, continue = true)

    When("The service is upgraded")
    val oldTask = marathon.tasks(serviceDef.id).value.head
    val update = marathon.updateApp(serviceDef.id, AppUpdate(env = Some(EnvVarValue(sys.env))))
    val newTask = eventually {
      marathon.tasks(serviceDef.id).value.find(_.id != oldTask.id).get
    }

    Then("The deployment does not succeed until the readiness checks succeed")
    val serviceFacade = new ServiceMockFacade(newTask)
    WaitTestSupport.waitUntil("ServiceMock is up", patienceConfig.timeout.totalNanos.nanos){ Try(serviceFacade.plan()).isSuccess }
    while (serviceFacade.plan().code != 200) {
      When("We continue on block until the plan is ready")
      serviceFacade.continue()
      marathon.listDeploymentsForBaseGroup().value should have size 1
    }
    waitForDeployment(update)
  }

  def deploy(service: AppDefinition, continue: Boolean): Unit = {
    Given("An application service")
    val result = marathon.createAppV2(service)
    result.code should be (201)
    val task = waitForTasks(service.id, 1).head //make sure, the app has really started
    val serviceFacade = new ServiceMockFacade(task)
    WaitTestSupport.waitUntil("ServiceMock is up", patienceConfig.timeout.totalNanos.nanos){ Try(serviceFacade.plan()).isSuccess }

    while (continue && serviceFacade.plan().code != 200) {
      When("We continue on block until the plan is ready")
      marathon.listDeploymentsForBaseGroup().value should have size 1
      serviceFacade.continue()
    }

    Then("The deployment should finish")
    waitForDeployment(result)
  }

  def serviceProxy(appId: PathId, plan: String, withHealth: Boolean): AppDefinition = {
    AppDefinition(
      id = appId,
      cmd = Some(s"""$serviceMockScript '$plan'"""),
      executor = "//cmd",
      resources = Resources(cpus = 0.5, mem = 128.0),
      upgradeStrategy = UpgradeStrategy(0, 0),
      portDefinitions = Seq(PortDefinition(0, name = Some("http"))),
      healthChecks =
        if (withHealth)
          Set(MarathonHttpHealthCheck(
          path = Some("/ping"),
          portIndex = Some(PortReference(0)),
          interval = 2.seconds,
          timeout = 1.second))
        else Set.empty[HealthCheck],
      readinessChecks = Seq(ReadinessCheck(
        "ready",
        portName = "http",
        path = "/v1/plan",
        interval = 2.seconds,
        timeout = 1.second,
        preserveLastResponse = true))
    )
  }

  /**
    * Create a shell script that can start a service mock
    */
  private lazy val serviceMockScript: String = {
    val javaExecutable = sys.props.get("java.home").fold("java")(_ + "/bin/java")
    val classPath = sys.props.getOrElse("java.class.path", "target/classes").replaceAll(" ", "")
    val main = classOf[ServiceMock].getName
    val run = s"""$javaExecutable -Xmx64m -classpath $classPath $main"""
    val file = File.createTempFile("serviceProxy", ".sh")
    file.deleteOnExit()

    FileUtils.write(
      file,
      s"""#!/bin/sh
          |set -x
          |exec $run $$*""".stripMargin)
    file.setExecutable(true)
    file.getAbsolutePath
  }
}
