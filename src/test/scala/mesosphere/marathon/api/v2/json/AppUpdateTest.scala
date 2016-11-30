package mesosphere.marathon
package api.v2.json

import com.wix.accord._
import mesosphere.Unstable
import mesosphere.marathon.api.JsonTestHelper
import mesosphere.marathon.api.v2.Validation.validateOrThrow
import mesosphere.marathon.api.v2.{ AppNormalization, AppsResource, ValidationHelper }
import mesosphere.marathon.api.v2.validation.AppValidation
import mesosphere.marathon.core.readiness.ReadinessCheckTestHelper
import mesosphere.marathon.raml.{ AppCContainer, AppUpdate, Artifact, Container, ContainerPortMapping, DockerContainer, EngineType, Environment, Network, NetworkMode, PortDefinition, PortDefinitions, Raml, SecretDef, UpgradeStrategy }
import mesosphere.marathon.state._
import mesosphere.marathon.state.PathId._
import mesosphere.marathon.test.MarathonSpec
import org.apache.mesos.{ Protos => Mesos }
import org.scalatest.Matchers
import play.api.libs.json.Json

import scala.collection.immutable.Seq

class AppUpdateTest extends MarathonSpec with Matchers {
  implicit val appUpdateValidator: Validator[AppUpdate] = AppValidation.validateCanonicalAppUpdateAPI(Set("secrets"))

  val runSpecId = PathId("/test")

  def shouldViolate(update: AppUpdate, path: String, template: String): Unit = {
    val violations = validate(update)
    assert(violations.isFailure)
    assert(ValidationHelper.getAllRuleConstrains(violations).exists(v =>
      v.path.getOrElse(false) == path && v.message == template
    ), s"expected path $path and message $template, but instead found" +
      ValidationHelper.getAllRuleConstrains(violations))
  }

  def shouldNotViolate(update: AppUpdate, path: String, template: String): Unit = {
    val violations = validate(update)
    assert(!ValidationHelper.getAllRuleConstrains(violations).exists(v =>
      v.path.getOrElse(false) == path && v.message == template))
  }

  test("Validation") {
    val update = AppUpdate()

    shouldViolate(
      update.copy(portDefinitions = Some(PortDefinitions(9000, 8080, 9000))),
      "/portDefinitions",
      "Ports must be unique."
    )

    shouldViolate(
      update.copy(portDefinitions = Some(Seq(
        PortDefinition(9000, name = Some("foo")),
        PortDefinition(9001, name = Some("foo"))))
      ),
      "/portDefinitions",
      "Port names must be unique."
    )

    shouldNotViolate(
      update.copy(portDefinitions = Some(Seq(
        PortDefinition(9000, name = Some("foo")),
        PortDefinition(9001, name = Some("bar"))))
      ),
      "/portDefinitions",
      "Port names must be unique."
    )

    shouldViolate(update.copy(mem = Some(-3.0)), "/mem", "got -3.0, expected 0.0 or more")
    shouldViolate(update.copy(cpus = Some(-3.0)), "/cpus", "got -3.0, expected 0.0 or more")
    shouldViolate(update.copy(disk = Some(-3.0)), "/disk", "got -3.0, expected 0.0 or more")
    shouldViolate(update.copy(instances = Some(-3)), "/instances", "got -3, expected 0 or more")
  }

  test("Validate secrets") {
    val update = AppUpdate()

    shouldViolate(update.copy(secrets = Some(Map(
      "a" -> SecretDef("")
    ))), "/secrets(a)/source", "must not be empty")

    shouldViolate(update.copy(secrets = Some(Map(
      "" -> SecretDef("a/b/c")
    ))), "/secrets()/", "must not be empty")
  }

  /**
    * @return an [[AppUpdate]] that's been normalized to canonical form
    */
  private[this] def fromJsonString(json: String): AppUpdate = {
    val update: AppUpdate = Json.fromJson[AppUpdate](Json.parse(json)).get
    AppNormalization.forDeprecatedFields(validateOrThrow(update)(AppValidation.validateOldAppUpdateAPI))
  }

  test("SerializationRoundtrip for empty definition") {
    val update0 = AppUpdate(container = Some(Container(EngineType.Mesos)))
    JsonTestHelper.assertSerializationRoundtripWorks(update0)
  }

  test("SerializationRoundtrip for definition with simple AppC container") {
    val update0 = AppUpdate(container = Some(Container(EngineType.Mesos, appc = Some(AppCContainer(
      image = "anImage",
      labels = Map("key" -> "foo", "value" -> "bar")
    )))))
    JsonTestHelper.assertSerializationRoundtripWorks(update0)
  }

  test("SerializationRoundtrip for extended definition", Unstable) {
    val update1 = AppUpdate(
      cmd = Some("sleep 60"),
      args = None,
      user = Some("nobody"),
      env = Some(Environment("LANG" -> "en-US")),
      instances = Some(16),
      cpus = Some(2.0),
      mem = Some(256.0),
      disk = Some(1024.0),
      executor = Some("/opt/executors/bin/some.executor"),
      constraints = Some(Nil),
      fetch = Some(Seq(Artifact(uri = "http://dl.corp.org/prodX-1.2.3.tgz"))),
      backoffSeconds = Some(2),
      backoffFactor = Some(1.2),
      maxLaunchDelaySeconds = Some(60),
      container = Some(Container(
        EngineType.Docker,
        docker = Some(DockerContainer(
          image = "docker:///group/image"
        )),
        portMappings = Seq(
          ContainerPortMapping(containerPort = 80, name = Some("http"))
        )
      )),
      healthChecks = Some(Seq.empty),
      taskKillGracePeriodSeconds = Some(2),
      dependencies = Some(Set.empty),
      upgradeStrategy = Some(UpgradeStrategy(1, 1)),
      labels = Some(
        Map(
          "one" -> "aaa",
          "two" -> "bbb",
          "three" -> "ccc"
        )
      ),
      networks = Some(Seq(Network(mode = NetworkMode.Container, labels = Map(
        "foo" -> "bar",
        "baz" -> "buzz"
      ))))
    )
    JsonTestHelper.assertSerializationRoundtripWorks(update1)
  }

  test("Serialization result of empty container") {
    val update2 = AppUpdate(container = None)
    val json2 =
      """
      {
        "cmd": null,
        "user": null,
        "env": null,
        "instances": null,
        "cpus": null,
        "mem": null,
        "disk": null,
        "executor": null,
        "constraints": null,
        "uris": null,
        "ports": null,
        "backoffSeconds": null,
        "backoffFactor": null,
        "container": null,
        "healthChecks": null,
        "dependencies": null,
        "version": null
      }
    """
    val readResult2 = fromJsonString(json2)
    assert(readResult2 == update2)
  }

  test("Serialization result of empty ipAddress") {
    val update2 = AppUpdate(ipAddress = None)
    val json2 =
      """
      {
        "cmd": null,
        "user": null,
        "env": null,
        "instances": null,
        "cpus": null,
        "mem": null,
        "disk": null,
        "executor": null,
        "constraints": null,
        "uris": null,
        "ports": null,
        "backoffSeconds": null,
        "backoffFactor": null,
        "container": null,
        "healthChecks": null,
        "dependencies": null,
        "ipAddress": null,
        "version": null
      }
      """
    val readResult2 = fromJsonString(json2)
    assert(readResult2 == update2)
  }

  test("Empty json corresponds to default instance") {
    val update3 = AppUpdate()
    val json3 = "{}"
    val readResult3 = fromJsonString(json3)
    assert(readResult3 == update3)
  }

  test("Args are correctly read") {
    val update4 = AppUpdate(args = Some(Seq("a", "b", "c")))
    val json4 = """{ "args": ["a", "b", "c"] }"""
    val readResult4 = fromJsonString(json4)
    assert(readResult4 == update4)
  }

  test("acceptedResourceRoles of update is only applied when != None") {
    val app = AppDefinition(id = PathId("withAcceptedRoles"), acceptedResourceRoles = Set("a"))

    val unchanged = Raml.fromRaml(Raml.fromRaml((AppUpdate(), app))).copy(versionInfo = app.versionInfo)
    assert(unchanged == app)

    val changed = Raml.fromRaml(Raml.fromRaml((AppUpdate(acceptedResourceRoles = Some(Set("b"))), app))).copy(versionInfo = app.versionInfo)
    assert(changed == app.copy(acceptedResourceRoles = Set("b")))
  }

  test("AppUpdate does not change existing versionInfo", Unstable) {
    // TODO(jdef) marked unstable but it really needs moving to AppsResourceTest, and should test that code for
    // preserving the version (see updateOrCreate)
    val app = AppDefinition(
      id = PathId("test"),
      cmd = Some("sleep 1"),
      versionInfo = VersionInfo.forNewConfig(Timestamp(1))
    )

    val updateCmd = AppUpdate(cmd = Some("sleep 2"))
    assert(Raml.fromRaml(Raml.fromRaml((updateCmd, app))).versionInfo == app.versionInfo)
  }

  test("AppUpdate with a version and other changes are not allowed") {
    val vfe = intercept[ValidationFailedException](validateOrThrow(
      AppUpdate(id = Some("/test"), cmd = Some("sleep 2"), version = Some(Timestamp(2).toOffsetDateTime))))
    assert(vfe.failure.violations.toString.contains("The 'version' field may only be combined with the 'id' field."))
  }

  test("update may not have both uris and fetch") {
    val json =
      """
      {
        "id": "app-with-network-isolation",
        "uris": ["http://example.com/file1.tar.gz"],
        "fetch": [{"uri": "http://example.com/file1.tar.gz"}]
      }
      """

    val vfe = intercept[ValidationFailedException](validateOrThrow(fromJsonString(json)))
    assert(vfe.failure.violations.toString.contains("may not be set in conjunction with fetch"))
  }

  test("update may not have both ports and portDefinitions") {
    val json =
      """
      {
        "id": "app",
        "ports": [1],
        "portDefinitions": [{"port": 2}]
      }
      """

    val vfe = intercept[ValidationFailedException](validateOrThrow(fromJsonString(json)))
    assert(vfe.failure.violations.toString.contains("cannot specify both ports and port definitions"))
  }

  test("update may not have duplicated ports") {
    val json =
      """
      {
        "id": "app",
        "ports": [1, 1]
      }
      """

    val vfe = intercept[ValidationFailedException](validateOrThrow(fromJsonString(json)))
    assert(vfe.failure.violations.toString.contains("ports must be unique"))
  }

  test("update JSON serialization preserves readiness checks") {
    val update = AppUpdate(
      id = Some("/test"),
      readinessChecks = Some(Seq(ReadinessCheckTestHelper.alternativeHttpsRaml))
    )
    val json = Json.toJson(update)
    val reread = json.as[AppUpdate]
    assert(reread == update)
  }

  test("update readiness checks are applied to app") {
    val update = AppUpdate(
      id = Some("/test"),
      readinessChecks = Some(Seq(ReadinessCheckTestHelper.alternativeHttpsRaml))
    )
    val app = AppDefinition(id = PathId("/test"))
    val updated = Raml.fromRaml(Raml.fromRaml((update, app)))

    assert(update.readinessChecks.map(_.map(Raml.fromRaml(_))).contains(updated.readinessChecks))
  }

  test("empty app updateStrategy on persistent volumes") {
    val json =
      """
      {
        "cmd": "sleep 1000",
        "container": {
          "type": "MESOS",
          "volumes": [
            {
              "containerPath": "home",
              "mode": "RW",
              "persistent": {
                "size": 100
                }
              }]
        },
        "residency": {
          "relaunchEscalationTimeoutSeconds": 10,
          "taskLostBehavior": "WAIT_FOREVER"
        }
      }
      """

    val update = fromJsonString(json)
    val strategy = AppsResource.withoutPriorAppDefinition(update, "foo".toPath).upgradeStrategy
    assert(strategy.contains(raml.UpgradeStrategy(
      minimumHealthCapacity = 0.5,
      maximumOverCapacity = 0
    )))
  }

  test("empty app residency on persistent volumes") {
    val json =
      """
      {
        "cmd": "sleep 1000",
        "container": {
          "type": "MESOS",
          "volumes": [
            {
              "containerPath": "home",
              "mode": "RW",
              "persistent": {
                "size": 100
                }
              }]
        },
        "upgradeStrategy": {
          "minimumHealthCapacity": 0.2,
          "maximumOverCapacity": 0
        }
      }
      """

    val update = fromJsonString(json)
    val residency = Raml.fromRaml(AppsResource.withoutPriorAppDefinition(update, "foo".toPath)).residency
    assert(residency.contains(Residency.defaultResidency))
  }

  test("empty app updateStrategy") {
    val json =
      """
      {
        "cmd": "sleep 1000",
        "container": {
          "type": "MESOS",
          "volumes": [
            {
              "containerPath": "home",
              "mode": "RW",
              "persistent": {
                "size": 100
                }
              }]
        },
        "residency": {
          "relaunchEscalationTimeoutSeconds": 10,
          "taskLostBehavior": "WAIT_FOREVER"
        }
      }
      """

    val update = fromJsonString(json)
    val strategy = AppsResource.withoutPriorAppDefinition(update, "foo".toPath).upgradeStrategy
    assert(strategy.contains(raml.UpgradeStrategy(
      minimumHealthCapacity = 0.5,
      maximumOverCapacity = 0
    )))
  }

  test("empty app persists container") {
    val json =
      """
        {
          "id": "/payload-id",
          "args": [],
          "container": {
            "type": "DOCKER",
            "volumes": [
              {
                "containerPath": "data",
                "mode": "RW",
                "persistent": {
                  "size": 100
                }
              }
            ],
            "docker": {
              "image": "anImage"
            }
          },
          "residency": {
            "taskLostBehavior": "WAIT_FOREVER",
            "relaunchEscalationTimeoutSeconds": 3600
          }
        }
      """

    val update = fromJsonString(json)
    val createdViaUpdate = Raml.fromRaml(AppsResource.withoutPriorAppDefinition(update, "/put-path-id".toPath))
    assert(update.container.isDefined)
    assert(createdViaUpdate.container.contains(state.Container.Docker(
      volumes = Seq(PersistentVolume("data", PersistentVolumeInfo(size = 100), mode = Mesos.Volume.Mode.RW)),
      image = "anImage"
    )), createdViaUpdate.container)
  }

  test("empty app persists existing residency") {
    val json =
      """
        {
          "id": "/app",
          "args": [],
          "container": {
            "type": "DOCKER",
            "volumes": [
              {
                "containerPath": "data",
                "mode": "RW",
                "persistent": {
                  "size": 100
                }
              }
            ],
            "docker": {
              "image": "anImage"
            }
          },
          "residency": {
            "taskLostBehavior": "WAIT_FOREVER",
            "relaunchEscalationTimeoutSeconds": 1234
          }
        }
      """

    val update = fromJsonString(json)
    val create = Raml.fromRaml(AppsResource.withoutPriorAppDefinition(update, "/app".toPath))
    assert(update.residency.isDefined)
    assert(update.residency.map(Raml.fromRaml(_)) == create.residency)
  }

  test("empty app persists existing upgradeStrategy") {
    val json =
      """
        {
          "id": "/app",
          "args": [],
          "container": {
            "type": "DOCKER",
            "volumes": [
              {
                "containerPath": "data",
                "mode": "RW",
                "persistent": {
                  "size": 100
                }
              }
            ],
            "docker": {
              "image": "anImage"
            }
          },
          "residency": {
            "taskLostBehavior": "WAIT_FOREVER",
            "relaunchEscalationTimeoutSeconds": 1234
          },
          "upgradeStrategy": {
            "minimumHealthCapacity": 0.1,
            "maximumOverCapacity": 0.0
          }
        }
      """

    val update = fromJsonString(json)
    val create = Raml.fromRaml(AppsResource.withoutPriorAppDefinition(update, "/app".toPath))
    assert(update.upgradeStrategy.isDefined)
    assert(update.upgradeStrategy.map(Raml.fromRaml(_)).contains(create.upgradeStrategy))
  }

  test("empty app residency") {
    val json =
      """
      {
        "cmd": "sleep 1000",
        "container": {
          "type": "MESOS",
          "volumes": [
            {
              "containerPath": "home",
              "mode": "RW",
              "persistent": {
                "size": 100
                }
              }]
        },
        "upgradeStrategy": {
          "minimumHealthCapacity": 0.2,
          "maximumOverCapacity": 0
        }
      }
      """

    val update = fromJsonString(json)
    val residency = Raml.fromRaml(AppsResource.withoutPriorAppDefinition(update, "foo".toPath)).residency
    assert(residency.contains(Residency.defaultResidency))
  }

  test("empty app update strategy on external volumes") {
    val json =
      """
      {
        "cmd": "sleep 1000",
        "container": {
          "type": "MESOS",
          "volumes": [
            {
              "containerPath": "/docker_storage",
              "mode": "RW",
              "external": {
                "name": "my-external-volume",
                "provider": "dvdi",
                "size": 1234
                }
              }]
        }
      }
      """

    val update = fromJsonString(json)
    val strategy = Raml.fromRaml(AppsResource.withoutPriorAppDefinition(update, "foo".toPath)).upgradeStrategy
    assert(strategy == state.UpgradeStrategy.forResidentTasks)
  }

  test("container change in AppUpdate should be stored") {
    val appDef = AppDefinition(id = runSpecId, container = Some(state.Container.Docker(image = "something")))
    // add port mappings..
    val appUpdate = AppUpdate(
      container = Some(Container(
        EngineType.Docker,
        docker = Some(DockerContainer(image = "something")),
        portMappings = Seq(
          ContainerPortMapping(containerPort = 4000)
        )
      ))
    )
    val roundTrip = Raml.fromRaml((appUpdate, appDef))
    roundTrip.container should be('nonEmpty)
    roundTrip.container.foreach { container =>
      container.portMappings should have size 1
      container.portMappings.head.containerPort should be (4000)
    }
  }
}
