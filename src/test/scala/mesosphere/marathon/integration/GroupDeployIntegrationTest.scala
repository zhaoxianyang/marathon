package mesosphere.marathon
package integration

import mesosphere.{ AkkaIntegrationFunTest, Unstable }
import mesosphere.marathon.integration.setup.{ EmbeddedMarathonTest, IntegrationHealthCheck, WaitTestSupport }
import mesosphere.marathon.raml.{ App, GroupUpdate, UpgradeStrategy }
import mesosphere.marathon.state.{ Group, PathId }
import org.apache.http.HttpStatus
import spray.http.DateTime

import scala.concurrent.duration._

@IntegrationTest
class GroupDeployIntegrationTest extends AkkaIntegrationFunTest with EmbeddedMarathonTest {

  //clean up state before running the test case
  before(cleanUp())

  test("create empty group successfully") {
    Given("A group which does not exist in marathon")
    val group = Group.emptyUpdate("test".toRootTestPath)

    When("The group gets created")
    val result = marathon.createGroup(group)

    Then("The group is created. A success event for this group is send.")
    result.code should be(201) //created
    waitForDeployment(result)
  }

  test("update empty group successfully") {
    Given("An existing group")
    val name = "test2".toRootTestPath
    val group = Group.emptyUpdate(name)
    val dependencies = Set("/test".toTestPath)
    waitForDeployment(marathon.createGroup(group))

    When("The group gets updated")
    waitForDeployment(marathon.updateGroup(name, group.copy(dependencies = Some(dependencies.map(_.toString)))))

    Then("The group is updated")
    val result = marathon.group("test2".toRootTestPath)
    result.code should be(200)
    result.value.dependencies should be(dependencies)
  }

  test("deleting an existing group gives a 200 http response") {
    Given("An existing group")
    val group = Group.emptyUpdate("test3".toRootTestPath)
    waitForDeployment(marathon.createGroup(group))

    When("The group gets deleted")
    val result = marathon.deleteGroup(PathId(group.id.get))
    waitForDeployment(result)

    Then("The group is deleted")
    result.code should be(200)
    // only expect the test base group itself
    marathon.listGroupsInBaseGroup.value.filter { group => group.id != testBasePath } should be('empty)
  }

  test("delete a non existing group should give a 404 http response") {
    When("A non existing group is deleted")
    val result = marathon.deleteGroup("does_not_exist".toRootTestPath)

    Then("We get a 404 http response code")
    result.code should be(404)
  }

  test("create a group with applications to start") {
    Given("A group with one application")
    val app = appProxy("/test/app".toRootTestPath, "v1", 2, healthCheck = None)
    val group = GroupUpdate(Some("/test".toRootTestPath.toString), apps = Some(Set(app)))

    When("The group is created")
    waitForDeployment(marathon.createGroup(group))

    Then("A success event is send and the application has been started")
    val tasks = waitForTasks(PathId(app.id), app.instances)
    tasks should have size 2
  }

  test("update a group with applications to restart") {
    Given("A group with one application started")
    val id = "test".toRootTestPath
    val appId = id / "app"
    val app1V1 = appProxy(appId, "v1", 2, healthCheck = None)
    waitForDeployment(marathon.createGroup(GroupUpdate(Some(id.toString), Some(Set(app1V1)))))
    waitForTasks(PathId(app1V1.id), app1V1.instances)

    When("The group is updated, with a changed application")
    val app1V2 = appProxy(appId, "v2", 2, healthCheck = None)
    waitForDeployment(marathon.updateGroup(id, GroupUpdate(Some(id.toString), Some(Set(app1V2)))))

    Then("A success event is send and the application has been started")
    waitForTasks(PathId(app1V2.id), app1V2.instances)
  }

  test("update a group with the same application so no restart is triggered") {
    Given("A group with one application started")
    val id = "test".toRootTestPath
    val appId = id / "app"
    val app1V1 = appProxy(appId, "v1", 2, healthCheck = None)
    waitForDeployment(marathon.createGroup(GroupUpdate(Some(id.toString), Some(Set(app1V1)))))
    waitForTasks(PathId(app1V1.id), app1V1.instances)
    val tasks = marathon.tasks(appId)

    When("The group is updated, with the same application")
    waitForDeployment(marathon.updateGroup(id, GroupUpdate(Some(id.toString), Some(Set(app1V1)))))

    Then("There is no deployment and all tasks still live")
    marathon.listDeploymentsForBaseGroup().value should be ('empty)
    marathon.tasks(appId).value.toSet should be(tasks.value.toSet)
  }

  test("create a group with application with health checks") {
    Given("A group with one application")
    val id = "proxy".toRootTestPath
    val appId = id / "app"
    val proxy = appProxy(appId, "v1", 1)
    val group = GroupUpdate(Some(id.toString), Some(Set(proxy)))

    When("The group is created")
    val create = marathon.createGroup(group)

    Then("A success event is send and the application has been started")
    waitForDeployment(create)
  }

  test("upgrade a group with application with health checks") {
    Given("A group with one application")
    val id = "test".toRootTestPath
    val appId = id / "app"
    val proxy = appProxy(appId, "v1", 1)
    val group = GroupUpdate(Some(id.toString), Some(Set(proxy)))
    waitForDeployment(marathon.createGroup(group))
    val check = appProxyCheck(PathId(proxy.id), "v1", state = true)

    When("The group is updated")
    check.afterDelay(1.second, state = false)
    check.afterDelay(3.seconds, state = true)
    val update = marathon.updateGroup(id, group.copy(apps = Some(Set(appProxy(appId, "v2", 1)))))

    Then("A success event is send and the application has been started")
    waitForDeployment(update)
  }

  test("rollback from an upgrade of group") {
    Given("A group with one application")
    val gid = "proxy".toRootTestPath
    val appId = gid / "app"
    val proxy = appProxy(appId, "v1", 2)
    val group = GroupUpdate(Some(gid.toString), Some(Set(proxy)))
    val create = marathon.createGroup(group)
    waitForDeployment(create)
    waitForTasks(PathId(proxy.id), proxy.instances)
    val v1Checks = appProxyCheck(appId, "v1", state = true)

    When("The group is updated")
    waitForDeployment(marathon.updateGroup(gid, group.copy(apps = Some(Set(appProxy(appId, "v2", 2))))))

    Then("The new version is deployed")
    val v2Checks = appProxyCheck(appId, "v2", state = true)
    WaitTestSupport.validFor("all v2 apps are available", 10.seconds) { v2Checks.pingSince(2.seconds) }

    When("A rollback to the first version is initiated")
    waitForDeployment(marathon.rollbackGroup(gid, create.value.version), 120.seconds)

    Then("The rollback will be performed and the old version is available")
    v1Checks.healthy
    WaitTestSupport.validFor("all v1 apps are available", 10.seconds) { v1Checks.pingSince(2.seconds) }
  }

  test("during Deployment the defined minimum health capacity is never undershot") {
    Given("A group with one application")
    val id = "test".toRootTestPath
    val appId = id / "app"
    val proxy = appProxy(appId, "v1", 2).copy(upgradeStrategy = Some(UpgradeStrategy(1, 1)))
    val group = GroupUpdate(Some(id.toString), Some(Set(proxy)))
    val create = marathon.createGroup(group)
    waitForDeployment(create)
    waitForTasks(appId, proxy.instances)
    val v1Check = appProxyCheck(appId, "v1", state = true)

    When("The new application is not healthy")
    val v2Check = appProxyCheck(appId, "v2", state = false) //will always fail
    val update = marathon.updateGroup(id, group.copy(apps = Some(Set(appProxy(appId, "v2", 2)))))

    Then("All v1 applications are kept alive")
    v1Check.healthy
    WaitTestSupport.validFor("all v1 apps are always available", 15.seconds) { v1Check.pingSince(3.seconds) }

    When("The new application becomes healthy")
    v2Check.state = true //make v2 healthy, so the app can be cleaned
    waitForDeployment(update)
  }

  test("An upgrade in progress can not be interrupted without force") {
    Given("A group with one application with an upgrade in progress")
    val id = "forcetest".toRootTestPath
    val appId = id / "app"
    val proxy = appProxy(appId, "v1", 2)
    val group = GroupUpdate(Some(id.toString), Some(Set(proxy)))
    val create = marathon.createGroup(group)
    waitForDeployment(create)
    appProxyCheck(appId, "v2", state = false) //will always fail
    marathon.updateGroup(id, group.copy(apps = Some(Set(appProxy(appId, "v2", 2)))))

    When("Another upgrade is triggered, while the old one is not completed")
    val result = marathon.updateGroup(id, group.copy(apps = Some(Set(appProxy(appId, "v3", 2)))))

    Then("An error is indicated")
    result.code should be (HttpStatus.SC_CONFLICT)
    waitForEvent("group_change_failed")

    When("Another upgrade is triggered with force, while the old one is not completed")
    val force = marathon.updateGroup(id, group.copy(apps = Some(Set(appProxy(appId, "v4", 2)))), force = true)

    Then("The update is performed")
    waitForDeployment(force)
  }

  test("A group with a running deployment can not be deleted without force") {
    Given("A group with one application with an upgrade in progress")
    val id = "forcetest".toRootTestPath
    val appId = id / "app"
    val proxy = appProxy(appId, "v1", 2)
    appProxyCheck(appId, "v1", state = false) //will always fail
    val group = GroupUpdate(Some(id.toString), Some(Set(proxy)))
    marathon.createGroup(group)

    When("Delete the group, while the deployment is in progress")
    val deleteResult = marathon.deleteGroup(id)

    Then("An error is indicated")
    deleteResult.code should be (HttpStatus.SC_CONFLICT)
    waitForEvent("group_change_failed")

    When("Delete is triggered with force, while the deployment is not completed")
    val force = marathon.deleteGroup(id, force = true)

    Then("The delete is performed")
    waitForDeployment(force)
  }

  test("Groups with Applications with circular dependencies can not get deployed") {
    Given("A group with 3 circular dependent applications")
    val db = appProxy("/test/db".toTestPath, "v1", 1, dependencies = Set("/test/frontend1".toTestPath))
    val service = appProxy("/test/service".toTestPath, "v1", 1, dependencies = Set(PathId(db.id)))
    val frontend = appProxy("/test/frontend1".toTestPath, "v1", 1, dependencies = Set(PathId(service.id)))
    val group = GroupUpdate(Some("test".toTestPath.toString), Some(Set(db, service, frontend)))

    When("The group gets posted")
    val result = marathon.createGroup(group)

    Then("An unsuccessful response has been posted, with an error indicating cyclic dependencies")
    val errors = (result.entityJson \ "details" \\ "errors").flatMap(_.as[Seq[String]])
    errors.find(_.contains("cyclic dependencies")) shouldBe defined
  }

  test("Applications with dependencies get deployed in the correct order") {
    Given("A group with 3 dependent applications")
    val db = appProxy("/test/db".toTestPath, "v1", 1)
    val service = appProxy("/test/service".toTestPath, "v1", 1, dependencies = Set(PathId(db.id)))
    val frontend = appProxy("/test/frontend1".toTestPath, "v1", 1, dependencies = Set(PathId(service.id)))
    val group = GroupUpdate(Some("/test".toTestPath.toString), Some(Set(db, service, frontend)))

    When("The group gets deployed")
    var ping = Map.empty[PathId, DateTime]
    def storeFirst(health: IntegrationHealthCheck): Unit = {
      if (!ping.contains(health.appId)) ping += health.appId -> DateTime.now
    }
    appProxyCheck(PathId(db.id), "v1", state = true).withHealthAction(storeFirst)
    appProxyCheck(PathId(service.id), "v1", state = true).withHealthAction(storeFirst)
    appProxyCheck(PathId(frontend.id), "v1", state = true).withHealthAction(storeFirst)
    waitForDeployment(marathon.createGroup(group))

    Then("The correct order is maintained")
    ping should have size 3
    ping(PathId(db.id)) should be < ping(PathId(service.id))
    ping(PathId(service.id)) should be < ping(PathId(frontend.id))
  }

  test("Groups with dependencies get deployed in the correct order") {
    Given("A group with 3 dependent applications")
    val db = appProxy("/test/db/db1".toTestPath, "v1", 1)
    val service = appProxy("/test/service/service1".toTestPath, "v1", 1)
    val frontend = appProxy("/test/frontend/frontend1".toTestPath, "v1", 1)
    val group = GroupUpdate(
      Some("/test".toTestPath.toString),
      Some(Set.empty[App]),
      Some(Set(
        GroupUpdate(Some("db"), apps = Some(Set(db))),
        GroupUpdate(Some("service"), apps = Some(Set(service))).copy(dependencies = Some(Set("/test/db".toTestPath.toString))),
        GroupUpdate(Some("frontend"), apps = Some(Set(frontend))).copy(dependencies = Some(Set("/test/service".toTestPath.toString)))
      ))
    )

    When("The group gets deployed")
    var ping = Map.empty[PathId, DateTime]
    def storeFirst(health: IntegrationHealthCheck): Unit = {
      if (!ping.contains(health.appId)) ping += health.appId -> DateTime.now
    }
    appProxyCheck(PathId(db.id), "v1", state = true).withHealthAction(storeFirst)
    appProxyCheck(PathId(service.id), "v1", state = true).withHealthAction(storeFirst)
    appProxyCheck(PathId(frontend.id), "v1", state = true).withHealthAction(storeFirst)
    waitForDeployment(marathon.createGroup(group))

    Then("The correct order is maintained")
    ping should have size 3
    ping(PathId(db.id)) should be < ping(PathId(service.id))
    ping(PathId(service.id)) should be < ping(PathId(frontend.id))
  }

  test("Groups with dependent applications get upgraded in the correct order with maintained upgrade strategy", Unstable) {
    var ping = Map.empty[String, DateTime]
    def key(health: IntegrationHealthCheck) = s"${health.appId}_${health.versionId}"
    def storeFirst(health: IntegrationHealthCheck): Unit = {
      if (!ping.contains(key(health))) ping += key(health) -> DateTime.now
    }
    def create(version: String, initialState: Boolean) = {
      val tolerateFiveMinutesOfFailures = appProxyHealthCheck(maxConsecutiveFailures = 300)
      val db = appProxy("/test/db".toTestPath, version, 1, healthCheck = Some(tolerateFiveMinutesOfFailures))
      val service = appProxy("/test/service".toTestPath, version, 1, dependencies = Set(PathId(db.id)), healthCheck = Some(tolerateFiveMinutesOfFailures))
      val frontend = appProxy("/test/frontend1".toTestPath, version, 1, dependencies = Set(PathId(service.id)), healthCheck = Some(tolerateFiveMinutesOfFailures))
      (
        GroupUpdate(Some("/test".toTestPath.toString), Some(Set(db, service, frontend))),
        appProxyCheck(PathId(db.id), version, state = initialState).withHealthAction(storeFirst),
        appProxyCheck(PathId(service.id), version, state = initialState).withHealthAction(storeFirst),
        appProxyCheck(PathId(frontend.id), version, state = initialState).withHealthAction(storeFirst)
      )
    }

    Given("A group with 3 dependent applications")
    val (groupV1, dbV1, serviceV1, frontendV1) = create("v1", initialState = true)
    waitForDeployment(marathon.createGroup(groupV1))

    When("The group gets updated, where frontend2 is not healthy")
    val (groupV2, dbV2, serviceV2, frontendV2) = create("v2", initialState = false)

    val upgrade = marathon.updateGroup(PathId(groupV2.id.get), groupV2)
    waitForHealthCheck(dbV2)

    Then("The correct order is maintained")
    ping should have size 4
    ping(key(dbV1)) should be < ping(key(serviceV1))
    ping(key(serviceV1)) should be < ping(key(frontendV1))
    WaitTestSupport.validFor("all v1 apps are available as well as db v2", 30.seconds) {
      dbV1.pingSince(2.seconds) &&
        serviceV1.pingSince(2.seconds) &&
        frontendV1.pingSince(2.seconds) &&
        dbV2.pingSince(2.seconds)
    }

    When("The v2 db becomes healthy")
    dbV2.state = true

    waitForHealthCheck(serviceV2)
    Then("The correct order is maintained")
    ping should have size 5
    ping(key(serviceV1)) should be < ping(key(frontendV1))
    ping(key(dbV2)) should be < ping(key(serviceV2))
    WaitTestSupport.validFor("service and frontend v1 are available as well as db and service v2", 30.seconds) {
      serviceV1.pingSince(2.seconds) &&
        frontendV1.pingSince(2.seconds) &&
        dbV2.pingSince(2.seconds) &&
        serviceV2.pingSince(2.seconds)
    }

    When("The v2 service becomes healthy")
    serviceV2.state = true

    waitForHealthCheck(frontendV2)
    Then("The correct order is maintained")
    ping should have size 6
    ping(key(dbV2)) should be < ping(key(serviceV2))
    ping(key(serviceV2)) should be < ping(key(frontendV2))
    WaitTestSupport.validFor("frontend v1 is available as well as all v2", 15.seconds) {
      frontendV1.pingSince(2.seconds) &&
        dbV2.pingSince(2.seconds) &&
        serviceV2.pingSince(2.seconds) &&
        frontendV2.pingSince(2.seconds)
    }

    When("The v2 frontend becomes healthy")
    frontendV2.state = true

    Then("The deployment can be finished. All v1 apps are destroyed and all v2 apps are healthy.")
    waitForDeployment(upgrade)
    List(dbV1, serviceV1, frontendV1).foreach(_.pinged = false)
    WaitTestSupport.validFor("all v2 apps are alive", 15.seconds) {
      !dbV1.pinged && !serviceV1.pinged && !frontendV1.pinged &&
        dbV2.pingSince(2.seconds) && serviceV2.pingSince(2.seconds) && frontendV2.pingSince(2.seconds)
    }
  }
}
