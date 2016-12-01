package mesosphere.marathon
package api.v2

import java.util
import javax.ws.rs.core.Response

import akka.event.EventStream
import mesosphere.marathon.api._
import mesosphere.marathon.core.appinfo.AppInfo.Embed
import mesosphere.marathon.core.appinfo._
import mesosphere.marathon.core.base.ConstantClock
import mesosphere.marathon.core.group.GroupManager
import mesosphere.marathon.core.health.HealthCheckManager
import mesosphere.marathon.core.pod.ContainerNetwork
import mesosphere.marathon.core.plugin.PluginManager
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.raml.{ App, ContainerPortMapping, DockerContainer, DockerNetwork, EngineType, EnvVarValueOrSecret, IpAddress, IpDiscovery, IpDiscoveryPort, Network, NetworkMode, Raml, SecretDef }
import mesosphere.marathon.state.PathId._
import mesosphere.marathon.state._
import mesosphere.marathon.storage.repository.{ AppRepository, GroupRepository, TaskFailureRepository }
import mesosphere.marathon.test.{ GroupCreation, MarathonActorSupport, MarathonSpec, Mockito }
import mesosphere.marathon.upgrade.DeploymentPlan
import org.scalatest.{ GivenWhenThen, Matchers }
import play.api.libs.json.{ JsNumber, JsObject, Json }

import scala.collection.immutable
import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

class AppsResourceTest extends MarathonSpec with MarathonActorSupport with Matchers with Mockito with GivenWhenThen with GroupCreation {

  def normalize(app: App)(implicit config: AppNormalization.Config = AppNormalization.Config(None)): App = {
    val migrated = AppNormalization.forDeprecatedFields(app)
    AppNormalization(migrated, config)
  }

  def normalizeAndConvert(
    app: App)(implicit config: AppNormalization.Config = AppNormalization.Config(None)): AppDefinition = {

    val normalized = normalize(app)(config)
    Raml.fromRaml(normalized)
  }

  def prepareApp(
    app: App)(implicit config: AppNormalization.Config = AppNormalization.Config(None)): (Array[Byte], DeploymentPlan) = {

    val normed = normalize(app)
    val appDef = Raml.fromRaml(normed)
    val rootGroup = createRootGroup(Map(appDef.id -> appDef))
    val plan = DeploymentPlan(rootGroup, rootGroup)
    val body = Json.stringify(Json.toJson(normed)).getBytes("UTF-8")
    groupManager.updateApp(any, any, any, any, any) returns Future.successful(plan)
    groupManager.rootGroup() returns Future.successful(rootGroup)
    groupManager.app(appDef.id) returns Future.successful(Some(appDef))
    (body, plan)
  }

  test("Create a new app successfully") {
    Given("An app and group")
    val app = App(id = "/app", cmd = Some("cmd"))
    val (body, plan) = prepareApp(app)

    When("The create request is made")
    clock += 5.seconds
    val result = Try {
      val response = appsResource.create(body, force = false, auth.request)

      Then("It is successful")
      response.getStatus should be(201)
      response.getMetadata.containsKey(RestResource.DeploymentHeader) should be(true)

      And("the JSON is as expected, including a newly generated version")
      import mesosphere.marathon.api.v2.json.Formats._
      val expected = AppInfo(
        normalizeAndConvert(app).copy(versionInfo = VersionInfo.OnlyVersion(clock.now())),
        maybeTasks = Some(immutable.Seq.empty),
        maybeCounts = Some(TaskCounts.zero),
        maybeDeployments = Some(immutable.Seq(Identifiable(plan.id)))
      )
      JsonTestHelper.assertThatJsonString(response.getEntity.asInstanceOf[String]).correspondsToJsonOf(expected)
    }
    if (!result.isSuccess) {
      result.failed.foreach {
        case v: ValidationFailedException =>
          assert(result.isSuccess, s"JSON body = ${new String(body)} :: violations = ${v.failure.violations}")
        case th =>
          throw th
      }
    }
  }

  test("Create a new app with IP/CT, no default network name, Alice does not specify a network") {
    Given("An app and group")
    val app = App(
      id = "/app",
      cmd = Some("cmd"),
      networks = Seq(Network(mode = NetworkMode.Container))
    )
    val result = Try(prepareApp(app))
    assert(result.isFailure && result.failed.get.getMessage.contains("network must specify a name"))
  }

  test("Create a new app with IP/CT on virtual network foo") {
    Given("An app and group")
    val app = App(
      id = "/app",
      cmd = Some("cmd"),
      networks = Seq(Network(mode = NetworkMode.Container, name = Some("foo")))
    )
    val (body, plan) = prepareApp(app)

    When("The create request is made")
    clock += 5.seconds
    val response = appsResource.create(body, force = false, auth.request)

    Then("It is successful")
    assert(response.getStatus == 201, s"body = ${new String(body)}, response = ${response.getEntity.asInstanceOf[String]}")
    response.getMetadata.containsKey(RestResource.DeploymentHeader) should be(true)

    And("the JSON is as expected, including a newly generated version")
    import mesosphere.marathon.api.v2.json.Formats._
    val expected = AppInfo(
      normalizeAndConvert(app).copy(versionInfo = VersionInfo.OnlyVersion(clock.now())),
      maybeTasks = Some(immutable.Seq.empty),
      maybeCounts = Some(TaskCounts.zero),
      maybeDeployments = Some(immutable.Seq(Identifiable(plan.id)))
    )
    JsonTestHelper.assertThatJsonString(response.getEntity.asInstanceOf[String]).correspondsToJsonOf(expected)
  }

  test("Create a new app with IP/CT on virtual network foo w/ MESOS container spec") {
    Given("An app and group")
    val app = App(
      id = "/app",
      cmd = Some("cmd"),
      networks = Seq(Network(mode = NetworkMode.Container, name = Some("foo"))),
      container = Some(raml.Container(`type` = EngineType.Mesos))
    )
    val (body, plan) = prepareApp(app)

    When("The create request is made")
    clock += 5.seconds
    val response = appsResource.create(body, force = false, auth.request)

    Then("It is successful")
    response.getStatus should be(201)
    response.getMetadata.containsKey(RestResource.DeploymentHeader) should be(true)

    And("the JSON is as expected, including a newly generated version")
    import mesosphere.marathon.api.v2.json.Formats._
    val expected = AppInfo(
      normalizeAndConvert(app).copy(versionInfo = VersionInfo.OnlyVersion(clock.now())),
      maybeTasks = Some(immutable.Seq.empty),
      maybeCounts = Some(TaskCounts.zero),
      maybeDeployments = Some(immutable.Seq(Identifiable(plan.id)))
    )
    JsonTestHelper.assertThatJsonString(response.getEntity.asInstanceOf[String]).correspondsToJsonOf(expected)
  }

  test("Create a new app with IP/CT on virtual network foo, then update it to bar") {
    Given("An app and group")
    val app = App(
      id = "/app",
      cmd = Some("cmd"),
      networks = Seq(Network(mode = NetworkMode.Container, name = Some("foo")))
    )
    prepareApp(app)

    When("The application is updated")
    val updatedApp = app.copy(networks = Seq(Network(mode = NetworkMode.Container, name = Some("bar"))))
    val updatedJson = Json.toJson(updatedApp).as[JsObject]
    val updatedBody = Json.stringify(updatedJson).getBytes("UTF-8")
    val response = appsResource.replace(updatedApp.id, updatedBody, false, auth.request)

    Then("It is successful")
    assert(response.getStatus == 200, s"response=${response.getEntity.toString}")
    response.getMetadata.containsKey(RestResource.DeploymentHeader) should be(true)
  }

  test("Create a new app with IP/CT on virtual network foo, then update it to nothing") {
    Given("An app and group")
    val app = App(
      id = "/app",
      cmd = Some("cmd"),
      networks = Seq(Network(mode = NetworkMode.Container, name = Some("foo")))
    )
    prepareApp(app)

    When("The application is updated")
    val updatedApp = app.copy(networks = Seq(Network(mode = NetworkMode.Container)))
    val updatedJson = Json.toJson(updatedApp).as[JsObject]
    val updatedBody = Json.stringify(updatedJson).getBytes("UTF-8")
    val response = appsResource.replace(updatedApp.id, updatedBody, false, auth.request)

    Then("It is successful")
    response.getStatus should be(200)
    response.getMetadata.containsKey(RestResource.DeploymentHeader) should be(true)
  }

  test("Create a new app without IP/CT when default virtual network is bar") {
    Given("An app and group")
    configArgs = Seq("--default_network_name", "bar")
    resetAppsResource()
    implicit val normalizationConfig = AppNormalization.Config(Some("bar"))

    val app = App(
      id = "/app",
      cmd = Some("cmd")
    )
    val (body, plan) = prepareApp(app)

    When("The create request is made")
    clock += 5.seconds
    val response = appsResource.create(body, force = false, auth.request)

    Then("It is successful")
    assert(response.getStatus == 201, s"body=${new String(body)}, response=${response.getEntity.asInstanceOf[String]}")
    response.getMetadata.containsKey(RestResource.DeploymentHeader) should be(true)

    And("the JSON is as expected, including a newly generated version")
    import mesosphere.marathon.api.v2.json.Formats._
    val expected = AppInfo(
      normalizeAndConvert(app).copy(versionInfo = VersionInfo.OnlyVersion(clock.now())),
      maybeTasks = Some(immutable.Seq.empty),
      maybeCounts = Some(TaskCounts.zero),
      maybeDeployments = Some(immutable.Seq(Identifiable(plan.id)))
    )
    JsonTestHelper.assertThatJsonString(response.getEntity.asInstanceOf[String]).correspondsToJsonOf(expected)
  }

  test("Create a new app with IP/CT when default virtual network is bar, Alice did not specify network name") {
    Given("An app and group")
    configArgs = Seq("--default_network_name", "bar")
    resetAppsResource()
    implicit val normalizationConfig = AppNormalization.Config(Some("bar"))

    val app = App(
      id = "/app",
      cmd = Some("cmd"),
      networks = Seq(Network(mode = NetworkMode.Container))
    )
    val (body, plan) = prepareApp(app)

    When("The create request is made")
    clock += 5.seconds
    val response = appsResource.create(body, force = false, auth.request)

    Then("It is successful")
    response.getStatus should be(201)
    response.getMetadata.containsKey(RestResource.DeploymentHeader) should be(true)

    And("the JSON is as expected, including a newly generated version")
    import mesosphere.marathon.api.v2.json.Formats._
    val expected = AppInfo(
      normalizeAndConvert(app).copy(
        versionInfo = VersionInfo.OnlyVersion(clock.now()),
        networks = Seq(ContainerNetwork(name = "bar"))
      ),
      maybeTasks = Some(immutable.Seq.empty),
      maybeCounts = Some(TaskCounts.zero),
      maybeDeployments = Some(immutable.Seq(Identifiable(plan.id)))
    )
    JsonTestHelper.assertThatJsonString(response.getEntity.asInstanceOf[String]).correspondsToJsonOf(expected)
  }

  test("Create a new app with IP/CT when default virtual network is bar, but Alice specified foo") {
    Given("An app and group")
    configArgs = Seq("--default_network_name", "bar")
    resetAppsResource()
    implicit val normalizationConfig = AppNormalization.Config(Some("bar"))

    val app = App(
      id = "/app",
      cmd = Some("cmd"),
      networks = Seq(Network(mode = NetworkMode.Container, name = Some("foo")))
    )
    val (body, plan) = prepareApp(app)

    When("The create request is made")
    clock += 5.seconds
    val response = appsResource.create(body, force = false, auth.request)

    Then("It is successful")
    response.getStatus should be(201)
    response.getMetadata.containsKey(RestResource.DeploymentHeader) should be(true)

    And("the JSON is as expected, including a newly generated version")
    import mesosphere.marathon.api.v2.json.Formats._
    val expected = AppInfo(
      normalizeAndConvert(app).copy(versionInfo = VersionInfo.OnlyVersion(clock.now())),
      maybeTasks = Some(immutable.Seq.empty),
      maybeCounts = Some(TaskCounts.zero),
      maybeDeployments = Some(immutable.Seq(Identifiable(plan.id)))
    )
    JsonTestHelper.assertThatJsonString(response.getEntity.asInstanceOf[String]).correspondsToJsonOf(expected)
  }

  test("Create a new app with IP/CT with virtual network foo w/ Docker") {
    Given("An app and group")
    val app = App(
      id = "/app",
      cmd = Some("cmd"),
      ipAddress = Some(IpAddress(networkName = Some("foo"))),
      container = Some(raml.Container(
        `type` = EngineType.Docker,
        docker = Some(DockerContainer(
          portMappings = Seq(ContainerPortMapping(containerPort = 0)),
          image = "jdef/helpme",
          network = Some(DockerNetwork.User)
        ))
      ))
    )
    val (body, plan) = prepareApp(app)

    When("The create request is made")
    clock += 5.seconds
    val response = appsResource.create(body, force = false, auth.request)

    Then("It is successful")
    response.getStatus should be(201)
    response.getMetadata.containsKey(RestResource.DeploymentHeader) should be(true)

    And("the JSON is as expected, including a newly generated version")
    import mesosphere.marathon.api.v2.json.Formats._
    val expected = AppInfo(
      normalizeAndConvert(app).copy(versionInfo = VersionInfo.OnlyVersion(clock.now())),
      maybeTasks = Some(immutable.Seq.empty),
      maybeCounts = Some(TaskCounts.zero),
      maybeDeployments = Some(immutable.Seq(Identifiable(plan.id)))
    )
    JsonTestHelper.assertThatJsonString(response.getEntity.asInstanceOf[String]).correspondsToJsonOf(expected)
  }

  test("Create a new app in BRIDGE mode w/ Docker") {
    Given("An app and group")
    val container = DockerContainer(
      network = Some(DockerNetwork.Bridge),
      image = "jdef/helpme",
      portMappings = Seq(
        ContainerPortMapping(containerPort = 0)
      )
    )

    val app = App(
      id = "/app",
      cmd = Some("cmd"),
      container = Some(raml.Container(`type` = EngineType.Docker, docker = Some(container))),
      portDefinitions = None
    )

    val appDef = normalizeAndConvert(app)
    val rootGroup = createRootGroup(Map(appDef.id -> appDef))
    val plan = DeploymentPlan(rootGroup, rootGroup)
    val body = Json.stringify(Json.toJson(app).as[JsObject]).getBytes("UTF-8")
    groupManager.updateApp(any, any, any, any, any) returns Future.successful(plan)
    groupManager.rootGroup() returns Future.successful(rootGroup)
    groupManager.app(appDef.id) returns Future.successful(Some(appDef))

    When("The create request is made")
    clock += 5.seconds
    val response = appsResource.create(body, force = false, auth.request)

    Then("It is successful")
    response.getStatus should be(201)
    response.getMetadata.containsKey(RestResource.DeploymentHeader) should be(true)

    And("the JSON is as expected, including a newly generated version")
    import mesosphere.marathon.api.v2.json.Formats._
    val containerDef = appDef.container
    val expected = AppInfo(
      appDef.copy(
        versionInfo = VersionInfo.OnlyVersion(clock.now()),
        container = containerDef.map(_.copyWith(
          portMappings = Seq(
            Container.PortMapping(containerPort = 0, hostPort = Some(0), protocol = "tcp")
          )
        ))
      ),
      maybeTasks = Some(immutable.Seq.empty),
      maybeCounts = Some(TaskCounts.zero),
      maybeDeployments = Some(immutable.Seq(Identifiable(plan.id)))
    )
    JsonTestHelper.assertThatJsonString(response.getEntity.asInstanceOf[String]).correspondsToJsonOf(expected)
  }

  test("Create a new app in USER mode w/ ipAddress.discoveryInfo w/ Docker") {
    Given("An app and group")
    val body =
      """
        | {
        |   "id": "/app",
        |   "cmd": "cmd",
        |   "ipAddress": {
        |     "networkName": "foo",
        |     "discovery": {
        |       "ports": [
        |         { "number": 1, "name": "bob", "protocol": "tcp" }
        |       ]
        |     }
        |   },
        |   "container": {
        |     "type": "DOCKER",
        |     "docker": {
        |       "image": "jdef/helpme",
        |       "portMappings": [
        |         { "containerPort": 0, "protocol": "tcp" }
        |       ]
        |     }
        |   },
        |   "portDefinitions": []
        | }
      """.stripMargin.getBytes

    When("The create request is made")
    clock += 5.seconds
    val response = appsResource.create(body, force = false, auth.request)

    Then("It is not successful")
    response.getStatus should be(422)
    response.getEntity.toString should include("/ipAddress")
    response.getEntity.toString should include("ipAddress/discovery is not allowed for Docker containers")
  }

  test("Create a new app in HOST mode w/ ipAddress.discoveryInfo w/ Docker") {
    Given("An app and group")
    val app = App(
      id = "/app",
      cmd = Some("cmd"),
      ipAddress = Some(IpAddress(
        networkName = Some("foo"),
        discovery = Some(IpDiscovery(ports = Seq(
          IpDiscoveryPort(number = 1, name = "bob")
        )))
      )),
      container = Some(raml.Container(
        `type` = EngineType.Docker,
        docker = Some(DockerContainer(
          network = Some(DockerNetwork.Host),
          image = "jdef/helpme"
        ))
      ))
    )
    // mixing ipAddress with Docker containers is not allowed by validation; API migration fails it too
    a[SerializationFailedException] shouldBe thrownBy(prepareApp(app))
  }

  test("Create a new app (that uses secrets) successfully") {
    Given("The secrets feature is enabled")
    configArgs = Seq("--enable_features", "secrets")
    resetAppsResource()

    And("An app with a secret and an envvar secret-ref")
    val app = App(id = "/app", cmd = Some("cmd"),
      secrets = Map[String, SecretDef]("foo" -> SecretDef("/bar")),
      env = Map[String, EnvVarValueOrSecret]("NAMED_FOO" -> raml.EnvVarSecretRef("foo")))
    val (body, plan) = prepareApp(app)

    When("The create request is made")
    clock += 5.seconds
    val response = appsResource.create(body, force = false, auth.request)

    Then("It is successful")
    response.getStatus should be(201)
    response.getMetadata.containsKey(RestResource.DeploymentHeader) should be(true)

    And("the JSON is as expected, including a newly generated version")
    import mesosphere.marathon.api.v2.json.Formats._
    val expected = AppInfo(
      normalizeAndConvert(app).copy(versionInfo = VersionInfo.OnlyVersion(clock.now())),
      maybeTasks = Some(immutable.Seq.empty),
      maybeCounts = Some(TaskCounts.zero),
      maybeDeployments = Some(immutable.Seq(Identifiable(plan.id)))
    )
    JsonTestHelper.assertThatJsonString(response.getEntity.asInstanceOf[String]).correspondsToJsonOf(expected)
  }

  test("Create a new app (that uses undefined secrets) and fails") {
    Given("The secrets feature is enabled")
    configArgs = Seq("--enable_features", "secrets")
    resetAppsResource()

    And("An app with an envvar secret-ref that does not point to an undefined secret")
    val app = App(id = "/app", cmd = Some("cmd"),
      env = Map[String, EnvVarValueOrSecret]("NAMED_FOO" -> raml.EnvVarSecretRef("foo")))
    val (body, _) = prepareApp(app)

    When("The create request is made")
    clock += 5.seconds
    val response = appsResource.create(body, force = false, auth.request)

    Then("It fails")
    response.getStatus should be(422)
    response.getEntity.toString should include("/env(NAMED_FOO)")
    response.getEntity.toString should include("references an undefined secret")
  }

  test("Create the secrets feature is NOT enabled an app (that uses secrets) fails") {
    Given("The secrets feature is NOT enabled")
    configArgs = Seq()
    resetAppsResource()
    config.isFeatureSet(Features.SECRETS) should be(false)

    And("An app with an envvar secret-ref that does not point to an undefined secret")
    val app = App(id = "/app", cmd = Some("cmd"),
      secrets = Map[String, SecretDef]("foo" -> SecretDef("/bar")),
      env = Map[String, EnvVarValueOrSecret]("NAMED_FOO" -> raml.EnvVarSecretRef("foo")))
    val (body, _) = prepareApp(app)

    When("The create request is made")
    clock += 5.seconds
    val response = appsResource.create(body, force = false, auth.request)

    Then("It fails")
    response.getStatus should be(422)
    response.getEntity.toString should include("Feature secrets is not enabled")
  }

  test("Create a new app fails with Validation errors for negative resources") {
    Given("An app with negative resources")

    {
      val app = App(id = "/app", cmd = Some("cmd"), mem = -128)
      val (body, plan) = prepareApp(app)

      Then("A constraint violation exception is thrown")
      val response = appsResource.create(body, false, auth.request)
      response.getStatus should be(422)
    }

    {
      val app = App(id = "/app", cmd = Some("cmd"), cpus = -1)
      val (body, _) = prepareApp(app)

      val response = appsResource.create(body, false, auth.request)
      response.getStatus should be(422)
    }

    {
      val app = App(id = "/app", cmd = Some("cmd"), instances = -1)
      val (body, _) = prepareApp(app)

      val response = appsResource.create(body, false, auth.request)
      response.getStatus should be(422)
    }
  }

  test("Create a new app successfully using ports instead of portDefinitions") {
    Given("An app and group")
    val app = App(
      id = "/app",
      cmd = Some("cmd"),
      portDefinitions = Some(Seq(raml.PortDefinition(1000), raml.PortDefinition(1001)))
    )
    val (_, plan) = prepareApp(app)
    val appJson = Json.toJson(app).as[JsObject]
    val body = Json.stringify(appJson).getBytes("UTF-8")

    When("The create request is made")
    clock += 5.seconds
    val response = appsResource.create(body, force = false, auth.request)

    Then("It is successful")
    assert(response.getStatus == 201, s"body=${new String(body)}, response=${response.getEntity.asInstanceOf[String]}")

    And("the JSON is as expected, including a newly generated version")
    import mesosphere.marathon.api.v2.json.Formats._
    val expected = AppInfo(
      normalizeAndConvert(app).copy(versionInfo = VersionInfo.OnlyVersion(clock.now())),
      maybeTasks = Some(immutable.Seq.empty),
      maybeCounts = Some(TaskCounts.zero),
      maybeDeployments = Some(immutable.Seq(Identifiable(plan.id)))
    )
    JsonTestHelper.assertThatJsonString(response.getEntity.asInstanceOf[String]).correspondsToJsonOf(expected)
  }

  test("Create a new app fails with Validation errors") {
    Given("An app with validation errors")
    val app = App(id = "/app")
    val (body, _) = prepareApp(app)

    Then("A constraint violation exception is thrown")
    val response = appsResource.create(body, false, auth.request)
    response.getStatus should be(422)
  }

  test("Create a new app with float instance count fails") {
    Given("The json of an invalid application")
    val invalidAppJson = Json.stringify(Json.obj("id" -> "/foo", "cmd" -> "cmd", "instances" -> 0.1))
    val rootGroup = createRootGroup()
    val plan = DeploymentPlan(rootGroup, rootGroup)
    groupManager.updateApp(any, any, any, any, any) returns Future.successful(plan)
    groupManager.rootGroup() returns Future.successful(rootGroup)

    Then("A constraint violation exception is thrown")
    val body = invalidAppJson.getBytes("UTF-8")
    intercept[RuntimeException] { appsResource.create(body, false, auth.request) }
  }

  test("Replace an existing application") {
    Given("An app and group")
    val app = AppDefinition(id = PathId("/app"), cmd = Some("foo"))
    val rootGroup = createRootGroup(Map(app.id -> app))
    val plan = DeploymentPlan(rootGroup, rootGroup)
    val body = """{ "cmd": "bla" }""".getBytes("UTF-8")
    groupManager.updateApp(any, any, any, any, any) returns Future.successful(plan)
    groupManager.app(PathId("/app")) returns Future.successful(Some(app))

    When("The application is updated")
    val response = appsResource.replace(app.id.toString, body, false, auth.request)

    Then("The application is updated")
    response.getStatus should be(200)
    response.getMetadata.containsKey(RestResource.DeploymentHeader) should be(true)
  }

  test("Replace an existing application using ports instead of portDefinitions") {
    Given("An app and group")
    val app = App(id = "/app", cmd = Some("foo"))
    prepareApp(app)

    val appJson = Json.toJson(app).as[JsObject]
    val appJsonWithOnlyPorts = appJson + ("ports" -> Json.parse("""[1000, 1001]"""))
    val body = Json.stringify(appJsonWithOnlyPorts).getBytes("UTF-8")

    When("The application is updated")
    val response = appsResource.replace(app.id, body, false, auth.request)

    Then("The application is updated")
    response.getStatus should be(200)
    response.getMetadata.containsKey(RestResource.DeploymentHeader) should be(true)
  }

  test("Replace an existing application fails due to docker container validation") {
    Given("An app update with an invalid container (missing docker field)")
    val app = App(id = "/app", cmd = Some("foo"))
    prepareApp(app)

    val body =
      """{
        |  "cmd": "sleep 1",
        |  "container": {
        |    "type": "DOCKER"
        |  }
        |}""".stripMargin.getBytes("UTF-8")

    Then("A validation exception is thrown")
    val response = appsResource.replace(app.id, body, force = false, auth.request)
    response.getStatus should be(422)
    response.getEntity.toString should include("/container/docker")
    response.getEntity.toString should include("not defined")
  }

  def createAppWithVolumes(`type`: String, volumes: String): Response = {
    val app = App(id = "/app", cmd = Some("foo"))
    prepareApp(app)

    val docker = if (`type` == "DOCKER") """"docker": {"image": "fop"},""" else ""
    val body =
      s"""
         |{
         |  "id": "external1",
         |  "cmd": "sleep 100",
         |  "instances": 1,
         |  "upgradeStrategy": { "minimumHealthCapacity": 0, "maximumOverCapacity": 0 },
         |  "container": {
         |    "type": "${`type`}",
         |    $docker
         |    $volumes
         |  }
         |}
      """.stripMargin

    When("The request is processed")
    appsResource.create(body.getBytes("UTF-8"), false, auth.request)
  }

  test("Creating an app with broken volume definition fails with readable error message") {
    Given("An app update with an invalid volume (wrong field name)")
    val response = createAppWithVolumes(
      "MESOS",
      """
        |    "volumes": [{
        |      "containerPath": "var",
        |      "persistent_WRONG_FIELD_NAME": {
        |        "size": 10
        |      },
        |      "mode": "RW"
        |    }]
      """.stripMargin)

    Then("The return code indicates that the hostPath of volumes[0] is missing") // although the wrong field should fail
    response.getStatus should be(422)
    response.getEntity.toString should include("/container/volumes(0)/hostPath")
    response.getEntity.toString should include("not defined")
  }

  test("Creating an app with an external volume for an illegal provider should fail") {
    Given("An app invalid volume (illegal volume provider)")
    val response = createAppWithVolumes(
      "MESOS",
      """
        |    "volumes": [{
        |      "containerPath": "var",
        |      "external": {
        |        "size": 10,
        |        "name": "foo",
        |        "provider": "acme"
        |      },
        |      "mode": "RW"
        |    }]
      """.stripMargin
    )

    Then("The return code indicates that the hostPath of volumes[0] is missing") // although the wrong field should fail
    response.getStatus should be(422)
    response.getEntity.toString should include("/container/volumes(0)/external/provider")
    response.getEntity.toString should include("is unknown provider")
  }

  test("Creating an app with an external volume with no name or provider name specified should FAIL provider validation") {
    Given("An app with an unnamed volume provider")
    val response = createAppWithVolumes(
      "MESOS",
      """
          |    "volumes": [{
          |      "containerPath": "var",
          |      "external": {
          |        "size": 10
          |      },
          |      "mode": "RW"
          |    }]
        """.stripMargin
    )

    Then("The return code indicates create failure")
    response.getStatus should be(422)

    val responseBody = response.getEntity.toString
    responseBody should include("/container/volumes(0)/external/provider")
    responseBody should include("/container/volumes(0)/external/name")
  }

  test("Creating an app with an external volume w/ MESOS and absolute containerPath should fail validation") {
    Given("An app with a named, non-'agent' volume provider")
    val response = createAppWithVolumes(
      "MESOS",
      """
        |    "volumes": [{
        |      "containerPath": "/var",
        |      "external": {
        |        "size": 10,
        |        "provider": "dvdi",
        |        "name": "namedfoo",
        |        "options": {"dvdi/driver": "bar"}
        |      },
        |      "mode": "RW"
        |    }]
      """.stripMargin
    )

    Then("The return code indicates create failure")
    response.getStatus should be(422)
    response.getEntity.toString should include("/container/volumes(0)/containerPath")
  }

  test("Creating an app with an external volume w/ MESOS and nested containerPath should fail validation") {
    Given("An app with a named, non-'agent' volume provider")
    val response = createAppWithVolumes(
      "MESOS",
      """
        |    "volumes": [{
        |      "containerPath": "var/child",
        |      "external": {
        |        "size": 10,
        |        "provider": "dvdi",
        |        "name": "namedfoo",
        |        "options": {"dvdi/driver": "bar"}
        |      },
        |      "mode": "RW"
        |    }]
      """.stripMargin
    )

    Then("The return code indicates create failure")
    response.getStatus should be(422)
    response.getEntity.toString should include("/container/volumes(0)/containerPath")
  }

  test("Creating an app with an external volume and MESOS containerizer should pass validation") {
    Given("An app with a named, non-'agent' volume provider")
    val response = createAppWithVolumes(
      "MESOS",
      """
        |    "volumes": [{
        |      "containerPath": "var",
        |      "external": {
        |        "size": 10,
        |        "provider": "dvdi",
        |        "name": "namedfoo",
        |        "options": {"dvdi/driver": "bar"}
        |      },
        |      "mode": "RW"
        |    }]
      """.stripMargin
    )

    Then("The return code indicates create success")
    assert(response.getStatus == 201, s"response=${response.getEntity.asInstanceOf[String]}")
    response.getMetadata.containsKey(RestResource.DeploymentHeader) should be(true)
  }

  test("Creating an app with an external volume using an invalid rexray option should fail") {
    Given("An app with a named, non-'agent' volume provider")
    val response = createAppWithVolumes(
      "MESOS",
      """
        |    "volumes": [{
        |      "containerPath": "var",
        |      "external": {
        |        "size": 10,
        |        "provider": "dvdi",
        |        "name": "namedfoo",
        |        "options": {"dvdi/driver": "rexray", "dvdi/iops": "0"}
        |      },
        |      "mode": "RW"
        |    }]
      """.stripMargin
    )

    Then("The return code indicates validation error")
    response.getStatus should be(422)
    response.getEntity.toString should include("/container/volumes(0)/external/options(\\\"dvdi/iops\\\")")
  }

  test("Creating an app with an external volume w/ relative containerPath DOCKER containerizer should fail validation") {
    Given("An app with a named, non-'agent' volume provider")
    val response = createAppWithVolumes(
      "DOCKER",
      """
        |    "volumes": [{
        |      "containerPath": "var",
        |      "external": {
        |        "provider": "dvdi",
        |        "name": "namedfoo",
        |        "options": {"dvdi/driver": "bar"}
        |      },
        |      "mode": "RW"
        |    }]
      """.stripMargin
    )

    Then("The return code indicates create failed")
    response.getStatus should be(422)
    response.getEntity.toString should include("/container/volumes(0)/containerPath")
  }

  test("Creating an app with an external volume and DOCKER containerizer should pass validation") {
    Given("An app with a named, non-'agent' volume provider")
    val response = createAppWithVolumes(
      "DOCKER",
      """
        |    "volumes": [{
        |      "containerPath": "/var",
        |      "external": {
        |        "provider": "dvdi",
        |        "name": "namedfoo",
        |        "options": {"dvdi/driver": "bar"}
        |      },
        |      "mode": "RW"
        |    }]
      """.stripMargin
    )

    Then("The return code indicates create success")
    response.getStatus should be(201)
    response.getMetadata.containsKey(RestResource.DeploymentHeader) should be(true)
  }

  test("Creating a DOCKER app with an external volume without driver option should NOT pass validation") {
    Given("An app with a named, non-'agent' volume provider")
    val response = createAppWithVolumes(
      "DOCKER",
      """
        |    "volumes": [{
        |      "containerPath": "/var",
        |      "external": {
        |        "provider": "dvdi",
        |        "name": "namedfoo",
        |        "options": {}
        |      },
        |      "mode": "RW"
        |    }]
      """.stripMargin
    )

    Then("The return code indicates create failure")
    response.getStatus should be(422)
    response.getEntity.toString should include("/container/volumes(0)/external/options(\\\"dvdi/driver\\\")")
    response.getEntity.toString should include("not defined")
  }

  test("Creating a Docker app with an external volume with size should fail validation") {
    Given("An app with a named, non-'agent' volume provider")
    val response = createAppWithVolumes(
      "DOCKER",
      """
        |    "volumes": [{
        |      "containerPath": "/var",
        |      "external": {
        |        "provider": "dvdi",
        |        "name": "namedfoo",
        |        "size": 42,
        |        "options": {
        |           "dvdi/driver": "rexray"
        |        }
        |      },
        |      "mode": "RW"
        |    }]
      """.stripMargin
    )

    println(response.getEntity.toString)

    Then("The return code indicates a validation error")
    response.getStatus should be(422)
    response.getEntity.toString should include("/container/volumes(0)/external/size")
    response.getEntity.toString should include("must be undefined")
  }

  test("Creating an app with an external volume, and docker volume and DOCKER containerizer should pass validation") {
    Given("An app with a named, non-'agent' volume provider and a docker host volume")
    val response = createAppWithVolumes(
      "DOCKER",
      """
        |    "volumes": [{
        |      "containerPath": "/var",
        |      "external": {
        |        "provider": "dvdi",
        |        "name": "namedfoo",
        |        "options": {"dvdi/driver": "bar"}
        |      },
        |      "mode": "RW"
        |    },{
        |      "hostPath": "/ert",
        |      "containerPath": "/ert",
        |      "mode": "RW"
        |    }]
      """.stripMargin
    )

    Then("The return code indicates create success")
    response.getStatus should be(201)
    response.getMetadata.containsKey(RestResource.DeploymentHeader) should be(true)
  }

  test("Creating an app with a duplicate external volume name (unfortunately) passes validation") {
    // we'll need to mitigate this with documentation: probably deprecating support for using
    // volume names with non-persistent volumes.
    Given("An app with DOCKER containerizer and multiple references to the same named volume")
    val response = createAppWithVolumes(
      "DOCKER",
      """
        |    "volumes": [{
        |      "containerPath": "/var",
        |      "external": {
        |        "provider": "dvdi",
        |        "name": "namedfoo",
        |        "options": {"dvdi/driver": "bar"}
        |      },
        |      "mode": "RW"
        |    },{
        |      "hostPath": "namedfoo",
        |      "containerPath": "/ert",
        |      "mode": "RW"
        |    }]
      """.stripMargin
    )

    Then("The return code indicates create success")
    response.getStatus should be(201)
    response.getMetadata.containsKey(RestResource.DeploymentHeader) should be(true)
  }

  test("Replacing an existing application with a Mesos docker container passes validation") {
    Given("An app update to a Mesos container with a docker image")
    val app = App(id = "/app", cmd = Some("foo"))
    prepareApp(app)

    val body =
      """{
        |  "cmd": "sleep 1",
        |  "container": {
        |    "type": "MESOS",
        |    "docker": {
        |      "image": "/test:latest"
        |    }
        |  }
        |}""".stripMargin.getBytes("UTF-8")

    When("The application is updated")
    val response = appsResource.replace(app.id, body, force = false, auth.request)

    Then("The return code indicates success")
    response.getStatus should be(200)
    response.getMetadata.containsKey(RestResource.DeploymentHeader) should be(true)
  }

  test("Restart an existing app") {
    val app = AppDefinition(id = PathId("/app"))
    val rootGroup = createRootGroup(Map(app.id -> app))
    val plan = DeploymentPlan(rootGroup, rootGroup)
    service.deploy(any, any) returns Future.successful(())
    groupManager.app(PathId("/app")) returns Future.successful(Some(app))

    groupManager.updateApp(any, any, any, any, any) returns Future.successful(plan)
    val response = appsResource.restart(app.id.toString, force = true, auth.request)
    response.getStatus should be(200)
    response.getMetadata.containsKey(RestResource.DeploymentHeader) should be(true)
  }

  test("Restart a non existing app will fail") {
    val missing = PathId("/app")
    groupManager.app(PathId("/app")) returns Future.successful(None)

    groupManager.updateApp(any, any, any, any, any) returns Future.failed(UnknownAppException(missing))

    intercept[UnknownAppException] { appsResource.restart(missing.toString, force = true, auth.request) }
  }

  test("Index has counts and deployments by default (regression for #2171)") {
    Given("An app and group")
    val app = AppDefinition(id = PathId("/app"), cmd = Some("foo"))
    val expectedEmbeds: Set[Embed] = Set(Embed.Counts, Embed.Deployments)
    val appInfo = AppInfo(app, maybeDeployments = Some(Seq(Identifiable("deployment-123"))), maybeCounts = Some(TaskCounts(1, 2, 3, 4)))
    appInfoService.selectAppsBy(any, eq(expectedEmbeds)) returns Future.successful(Seq(appInfo))

    When("The the index is fetched without any filters")
    val response = appsResource.index(null, null, null, new java.util.HashSet(), auth.request)

    Then("The response holds counts and deployments")
    val appJson = Json.parse(response.getEntity.asInstanceOf[String])
    (appJson \ "apps" \\ "deployments" head) should be (Json.arr(Json.obj("id" -> "deployment-123")))
    (appJson \ "apps" \\ "tasksStaged" head) should be (JsNumber(1))
  }

  test("Search apps can be filtered") {
    val app1 = AppDefinition(id = PathId("/app/service-a"), cmd = Some("party hard"), labels = Map("a" -> "1", "b" -> "2"))
    val app2 = AppDefinition(id = PathId("/app/service-b"), cmd = Some("work hard"), labels = Map("a" -> "1", "b" -> "3"))
    val apps = Set(app1, app2)

    def search(cmd: Option[String], id: Option[String], label: Option[String]): Set[AppDefinition] = {
      val selector = appsResource.search(cmd, id, label)
      apps.filter(selector.matches)
    }

    search(cmd = None, id = None, label = None) should be(Set(app1, app2))
    search(cmd = Some(""), id = None, label = None) should be(Set(app1, app2))
    search(cmd = Some("party"), id = None, label = None) should be(Set(app1))
    search(cmd = Some("work"), id = None, label = None) should be(Set(app2))
    search(cmd = Some("hard"), id = None, label = None) should be(Set(app1, app2))
    search(cmd = Some("none"), id = None, label = None) should be(Set.empty)

    search(cmd = None, id = Some("app"), label = None) should be(Set(app1, app2))
    search(cmd = None, id = Some("service-a"), label = None) should be(Set(app1))
    search(cmd = Some("party"), id = Some("app"), label = None) should be(Set(app1))
    search(cmd = Some("work"), id = Some("app"), label = None) should be(Set(app2))
    search(cmd = Some("hard"), id = Some("service-a"), label = None) should be(Set(app1))
    search(cmd = Some(""), id = Some(""), label = None) should be(Set(app1, app2))

    search(cmd = None, id = None, label = Some("b==2")) should be(Set(app1))
    search(cmd = Some("party"), id = Some("app"), label = Some("a==1")) should be(Set(app1))
    search(cmd = Some("work"), id = Some("app"), label = Some("a==1")) should be(Set(app2))
    search(cmd = Some("hard"), id = Some("service-a"), label = Some("a==1")) should be(Set(app1))
    search(cmd = Some(""), id = Some(""), label = Some("")) should be(Set(app1, app2))
  }

  test("access without authentication is denied") {
    Given("An unauthenticated request")
    auth.authenticated = false
    val req = auth.request
    val embed = new util.HashSet[String]()
    val app = """{"id":"/a/b/c","cmd":"foo","ports":[]}"""
    groupManager.rootGroup() returns Future.successful(createRootGroup())

    When("we try to fetch the list of apps")
    val index = appsResource.index("", "", "", embed, req)
    Then("we receive a NotAuthenticated response")
    index.getStatus should be(auth.NotAuthenticatedStatus)

    When("we try to add an app")
    val create = appsResource.create(app.getBytes("UTF-8"), false, req)
    Then("we receive a NotAuthenticated response")
    create.getStatus should be(auth.NotAuthenticatedStatus)

    When("we try to fetch an app")
    val show = appsResource.show("", embed, req)
    Then("we receive a NotAuthenticated response")
    show.getStatus should be(auth.NotAuthenticatedStatus)

    When("we try to update an app")
    val replace = appsResource.replace("", app.getBytes("UTF-8"), false, req)
    Then("we receive a NotAuthenticated response")
    replace.getStatus should be(auth.NotAuthenticatedStatus)

    When("we try to update multiple apps")
    val replaceMultiple = appsResource.replaceMultiple(false, s"[$app]".getBytes("UTF-8"), req)
    Then("we receive a NotAuthenticated response")
    replaceMultiple.getStatus should be(auth.NotAuthenticatedStatus)

    When("we try to delete an app")
    val delete = appsResource.delete(false, "", req)
    Then("we receive a NotAuthenticated response")
    delete.getStatus should be(auth.NotAuthenticatedStatus)

    When("we try to restart an app")
    val restart = appsResource.restart("", false, req)
    Then("we receive a NotAuthenticated response")
    restart.getStatus should be(auth.NotAuthenticatedStatus)
  }

  test("access without authorization is denied") {
    Given("A real Group Manager with one app")
    useRealGroupManager()
    val appA = AppDefinition("/a".toRootPath)
    val rootGroup = createRootGroup(apps = Map(appA.id -> appA))
    groupRepository.root() returns Future.successful(rootGroup)

    Given("An unauthorized request")
    auth.authenticated = true
    auth.authorized = false
    val req = auth.request
    val embed = new util.HashSet[String]()
    val app = """{"id":"/a","cmd":"foo","ports":[]}"""

    When("we try to create an app")
    val create = appsResource.create(app.getBytes("UTF-8"), false, req)
    Then("we receive a NotAuthorized response")
    create.getStatus should be(auth.UnauthorizedStatus)

    When("we try to fetch an app")
    val show = appsResource.show("*", embed, req)
    Then("we receive a NotAuthorized response")
    show.getStatus should be(auth.UnauthorizedStatus)

    When("we try to update an app")
    val replace = appsResource.replace("/a", app.getBytes("UTF-8"), false, req)
    Then("we receive a NotAuthorized response")
    replace.getStatus should be(auth.UnauthorizedStatus)

    When("we try to update multiple apps")
    val replaceMultiple = appsResource.replaceMultiple(false, s"[$app]".getBytes("UTF-8"), req)
    Then("we receive a NotAuthorized response")
    replaceMultiple.getStatus should be(auth.UnauthorizedStatus)

    When("we try to remove an app")
    val delete = appsResource.delete(false, "/a", req)
    Then("we receive a NotAuthorized response")
    delete.getStatus should be(auth.UnauthorizedStatus)

    When("we try to restart an app")
    val restart = appsResource.restart("/a", false, req)
    Then("we receive a NotAuthorized response")
    restart.getStatus should be(auth.UnauthorizedStatus)
  }

  test("access with limited authorization gives a filtered apps listing") {
    Given("An authorized identity with limited ACL's")
    auth.authFn = (resource: Any) => {
      val id = resource match {
        case app: AppDefinition => app.id.toString
        case _ => resource.asInstanceOf[Group].id.toString
      }
      id.startsWith("/visible")
    }
    implicit val identity = auth.identity
    val selector = appsResource.selectAuthorized(Selector.forall(Seq.empty))
    val apps = Seq(
      AppDefinition("/visible/app".toPath),
      AppDefinition("/visible/other/foo/app".toPath),
      AppDefinition("/secure/app".toPath),
      AppDefinition("/root".toPath),
      AppDefinition("/other/great/app".toPath)
    )

    When("The selector selects applications")
    val filtered = apps.filter(selector.matches)

    Then("The list of filtered apps only contains apps according to ACL's")
    filtered should have size 2
    filtered.head should be (apps.head)
    filtered(1) should be (apps(1))
  }

  test("delete with authorization gives a 404 if the app doesn't exist") {
    Given("An authenticated identity with full access")
    auth.authenticated = true
    auth.authorized = false
    val req = auth.request

    When("We try to remove a non-existing application")
    useRealGroupManager()
    groupRepository.root returns Future.successful(createRootGroup())

    Then("A 404 is returned")
    intercept[UnknownAppException] { appsResource.delete(false, "/foo", req) }
  }

  var clock: ConstantClock = _
  var eventBus: EventStream = _
  var service: MarathonSchedulerService = _
  var taskTracker: InstanceTracker = _
  var taskKiller: TaskKiller = _
  var healthCheckManager: HealthCheckManager = _
  var taskFailureRepo: TaskFailureRepository = _
  var config: MarathonConf = _
  var groupManager: GroupManager = _
  var appInfoService: AppInfoService = _
  var appsResource: AppsResource = _
  var auth: TestAuthFixture = _
  var appRepository: AppRepository = _
  var appTaskResource: AppTasksResource = _
  var groupRepository: GroupRepository = _
  var configArgs: Seq[String] = _

  def resetAppsResource(): Unit = {
    config = AllConf.withTestConfig(configArgs: _*)
    appsResource = new AppsResource(
      clock,
      eventBus,
      appTaskResource,
      service,
      appInfoService,
      config,
      groupManager,
      PluginManager.None
    )(auth.auth, auth.auth)
  }

  before {
    configArgs = Seq("--enable_features", "external_volumes")
    clock = ConstantClock()
    auth = new TestAuthFixture
    eventBus = mock[EventStream]
    service = mock[MarathonSchedulerService]
    taskTracker = mock[InstanceTracker]
    taskKiller = mock[TaskKiller]
    healthCheckManager = mock[HealthCheckManager]
    taskFailureRepo = mock[TaskFailureRepository]
    appInfoService = mock[AppInfoService]
    groupManager = mock[GroupManager]
    appRepository = mock[AppRepository]
    appTaskResource = mock[AppTasksResource]
    resetAppsResource()
  }

  private[this] def useRealGroupManager(): Unit = {
    val f = new TestGroupManagerFixture()
    service = f.service
    config = f.config
    appRepository = f.appRepository
    groupManager = f.groupManager
    groupRepository = f.groupRepository

    appsResource = new AppsResource(
      clock,
      eventBus,
      appTaskResource,
      service,
      appInfoService,
      config,
      groupManager,
      PluginManager.None
    )(auth.auth, auth.auth)
  }
}
