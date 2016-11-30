package mesosphere.marathon
package api.v2.json

import akka.Done
import mesosphere.Unstable
import mesosphere.marathon.api.v2.AppNormalization
import mesosphere.marathon.api.v2.Validation
import mesosphere.marathon.api.v2.validation.AppValidation
import mesosphere.marathon.core.pod.ContainerNetwork
import mesosphere.marathon.core.readiness.ReadinessCheckTestHelper
import mesosphere.marathon.raml.Raml
import mesosphere.marathon.state.PathId._
import mesosphere.marathon.state.VersionInfo.OnlyVersion
import mesosphere.marathon.state._
import mesosphere.marathon.test.MarathonSpec
import org.scalatest.Matchers
import play.api.libs.json._

import scala.collection.immutable.Seq
import scala.concurrent.duration._

class AppDefinitionFormatsTest
    extends MarathonSpec
    with AppAndGroupFormats
    with HealthCheckFormats
    with Matchers {

  import Formats.PathIdFormat

  object Fixture {
    val a1 = AppDefinition(
      id = "app1".toRootPath,
      cmd = Some("sleep 10"),
      versionInfo = VersionInfo.OnlyVersion(Timestamp(1))
    )

    val j1 = Json.parse("""
      {
        "id": "app1",
        "cmd": "sleep 10",
        "version": "1970-01-01T00:00:00.001Z"
      }
    """)
  }

  def normalizeAndConvert(app: raml.App): AppDefinition =
    // this is roughly the equivalent of how the original Formats behaved
    Raml.fromRaml(
      Validation.validateOrThrow(
        AppNormalization.apply(
          Validation.validateOrThrow(
            AppNormalization.forDeprecatedFields(app))(AppValidation.validateOldAppAPI),
          AppNormalization.Config(None)
        ))(AppValidation.validateCanonicalAppAPI(Set.empty)))

  def assumeValid(f: => Done): Done = scala.util.Try { f }.recover {
    // handle RAML validation errors
    case vfe: ValidationFailedException =>
      assert(false, vfe.failure.violations)
      Done
    case th => throw th
  }.get

  test("ToJson") {
    import AppDefinition._
    import Fixture._
    import mesosphere.marathon.raml._

    val r1 = Json.toJson(a1)
    // check supplied values
    (r1 \ "id").get should equal (JsString("/app1"))
    (r1 \ "cmd").get should equal (JsString("sleep 10"))
    (r1 \ "version").get should equal (JsString("1970-01-01T00:00:00.001Z"))
    (r1 \ "versionInfo").asOpt[JsObject] should equal(None)

    // check default values
    (r1 \ "args").asOpt[Seq[String]] should be (empty)
    (r1 \ "user").asOpt[String] should equal (None)
    (r1 \ "env").as[Map[String, String]] should equal (DefaultEnv)
    (r1 \ "instances").as[Long] should equal (DefaultInstances)
    (r1 \ "cpus").as[Double] should equal (DefaultCpus)
    (r1 \ "mem").as[Double] should equal (DefaultMem)
    (r1 \ "disk").as[Double] should equal (DefaultDisk)
    (r1 \ "gpus").as[Int] should equal (DefaultGpus)
    (r1 \ "executor").as[String] should equal (DefaultExecutor)
    (r1 \ "constraints").as[Set[Seq[String]]] should equal (DefaultConstraints.map(_.toRaml[Seq[String]]))
    (r1 \ "fetch").as[Seq[raml.Artifact]] should equal (DefaultFetch.map(_.toRaml))
    (r1 \ "storeUrls").as[Seq[String]] should equal (DefaultStoreUrls)
    (r1 \ "portDefinitions").asOpt[Seq[raml.PortDefinition]] should equal (None)
    (r1 \ "requirePorts").as[Boolean] should equal (DefaultRequirePorts)
    (r1 \ "backoffSeconds").as[Long] should equal (DefaultBackoff.toSeconds)
    (r1 \ "backoffFactor").as[Double] should equal (DefaultBackoffFactor)
    (r1 \ "maxLaunchDelaySeconds").as[Long] should equal (DefaultMaxLaunchDelay.toSeconds)
    (r1 \ "container").asOpt[String] should equal (None)
    (r1 \ "healthChecks").as[Seq[raml.AppHealthCheck]] should equal (Raml.toRaml(DefaultHealthChecks))
    (r1 \ "dependencies").as[Set[PathId]] should equal (DefaultDependencies)
    (r1 \ "upgradeStrategy").as[UpgradeStrategy] should equal (DefaultUpgradeStrategy.toRaml)
    (r1 \ "residency").asOpt[String] should equal (None)
    (r1 \ "secrets").as[Map[String, raml.SecretDef]] should equal (DefaultSecrets.toRaml)
    (r1 \ "taskKillGracePeriodSeconds").asOpt[Long] should equal (DefaultTaskKillGracePeriod)
  }

  test("ToJson should serialize full version info") {
    import Fixture._

    val r1 = Json.toJson(a1.copy(versionInfo = VersionInfo.FullVersionInfo(
      version = Timestamp(3),
      lastScalingAt = Timestamp(2),
      lastConfigChangeAt = Timestamp(1)
    )))
    (r1 \ "version").as[String] should equal("1970-01-01T00:00:00.003Z")
    (r1 \ "versionInfo" \ "lastScalingAt").as[String] should equal("1970-01-01T00:00:00.002Z")
    (r1 \ "versionInfo" \ "lastConfigChangeAt").as[String] should equal("1970-01-01T00:00:00.001Z")
  }

  test("FromJson") {
    import AppDefinition._
    import Fixture._

    val raw = j1.as[raml.App]
    val r1 = normalizeAndConvert(raw)

    // check supplied values
    r1.id should equal (a1.id)
    r1.cmd should equal (a1.cmd)
    r1.version should equal (Timestamp(1))
    r1.versionInfo shouldBe a[VersionInfo.OnlyVersion]
    // check default values
    r1.args should equal (DefaultArgs)
    r1.user should equal (DefaultUser)
    r1.env should equal (DefaultEnv)
    r1.instances should equal (DefaultInstances)
    r1.resources.cpus should equal (DefaultCpus)
    r1.resources.mem should equal (DefaultMem)
    r1.resources.disk should equal (DefaultDisk)
    r1.resources.gpus should equal (DefaultGpus)
    r1.executor should equal (DefaultExecutor)
    r1.constraints should equal (DefaultConstraints)
    r1.fetch should equal (DefaultFetch)
    r1.storeUrls should equal (DefaultStoreUrls)
    r1.portDefinitions should equal (Seq(PortDefinition(port = 0)))
    r1.requirePorts should equal (DefaultRequirePorts)
    r1.backoffStrategy.backoff should equal (DefaultBackoff)
    r1.backoffStrategy.factor should equal (DefaultBackoffFactor)
    r1.backoffStrategy.maxLaunchDelay should equal (DefaultMaxLaunchDelay)
    r1.container should equal (DefaultContainer)
    r1.healthChecks should equal (DefaultHealthChecks)
    r1.dependencies should equal (DefaultDependencies)
    r1.upgradeStrategy should equal (DefaultUpgradeStrategy)
    r1.acceptedResourceRoles should be ('empty)
    r1.secrets should equal (DefaultSecrets)
    r1.taskKillGracePeriod should equal (DefaultTaskKillGracePeriod)
  }

  test("FromJSON should ignore VersionInfo") {
    val app = Json.parse(
      """{
        |  "id": "test",
        |  "cmd": "foo",
        |  "version": "1970-01-01T00:00:00.002Z",
        |  "versionInfo": {
        |     "lastScalingAt": "1970-01-01T00:00:00.002Z",
        |     "lastConfigChangeAt": "1970-01-01T00:00:00.001Z"
        |  }
        |}""".stripMargin).as[raml.App]

    assumeValid {
      val appDef = normalizeAndConvert(app)
      appDef.versionInfo shouldBe a[OnlyVersion]
      Done
    }
  }

  test("FromJSON should fail for empty id") {
    val json = Json.parse(""" { "id": "" }""")
    a[ValidationFailedException] shouldBe thrownBy { normalizeAndConvert(json.as[raml.App]) }
  }

  test("FromJSON should fail when using / as an id") {
    val json = Json.parse(""" { "id": "/" }""")
    a[ValidationFailedException] shouldBe thrownBy { normalizeAndConvert(json.as[raml.App]) }
  }

  test("FromJSON should not fail when 'cpus' is greater than 0") {
    val json = Json.parse(""" { "id": "test", "cmd": "foo", "cpus": 0.0001 }""")
    noException should be thrownBy {
      normalizeAndConvert(json.as[raml.App])
    }
  }

  test("""ToJSON should correctly handle missing acceptedResourceRoles""") {
    val appDefinition = AppDefinition(id = PathId("test"), acceptedResourceRoles = Set.empty)
    val json = Json.toJson(appDefinition)
    (json \ "acceptedResourceRoles").asOpt[Set[String]] should be(None)
  }

  test("""ToJSON should correctly handle acceptedResourceRoles""") {
    val appDefinition = AppDefinition(id = PathId("test"), acceptedResourceRoles = Set("a"))
    val json = Json.toJson(appDefinition)
    (json \ "acceptedResourceRoles").as[Set[String]] should be(Set("a"))
  }

  test("""FromJSON should parse "acceptedResourceRoles": ["production", "*"] """) {
    val json = Json.parse(""" { "id": "test", "cmd": "foo", "acceptedResourceRoles": ["production", "*"] }""")
    val appDef = normalizeAndConvert(json.as[raml.App])
    appDef.acceptedResourceRoles should equal(Set("production", ResourceRole.Unreserved))
  }

  test("""FromJSON should parse "acceptedResourceRoles": ["*"] """) {
    val json = Json.parse(""" { "id": "test", "cmd": "foo", "acceptedResourceRoles": ["*"] }""")
    val appDef = normalizeAndConvert(json.as[raml.App])
    appDef.acceptedResourceRoles should equal(Set(ResourceRole.Unreserved))
  }

  test("FromJSON should fail when 'acceptedResourceRoles' is defined but empty") {
    val json = Json.parse(""" { "id": "test", "cmd": "foo", "acceptedResourceRoles": [] }""")
    a[ValidationFailedException] shouldBe thrownBy { normalizeAndConvert(json.as[raml.App]) }
  }

  test("FromJSON should read the default upgrade strategy") {
    assumeValid {
      val json = Json.parse(""" { "id": "test", "cmd": "foo" }""")
      val appDef = normalizeAndConvert(json.as[raml.App])
      appDef.upgradeStrategy should be(UpgradeStrategy.empty)
      Done
    }
  }

  test("FromJSON should have a default residency upgrade strategy") {
    assumeValid {
      val json = Json.parse(
        """{
          |  "id": "test",
          |  "container": {
          |    "type": "DOCKER",
          |    "docker": { "image": "busybox" },
          |    "volumes": [
          |      { "containerPath": "b", "persistent": { "size": 1 }, "mode": "RW" }
          |    ]
          |  },
          |  "residency": {}
          |}""".stripMargin)
      val appDef = normalizeAndConvert(json.as[raml.App])
      appDef.upgradeStrategy should be(UpgradeStrategy.forResidentTasks)
      Done
    }
  }

  test("FromJSON should read the default residency automatically residency ") {
    assumeValid {
      val json = Json.parse(
        """
        |{
        |  "id": "resident",
        |  "cmd": "foo",
        |  "container": {
        |    "type": "MESOS",
        |    "volumes": [{
        |      "containerPath": "var",
        |      "persistent": { "size": 10 },
        |      "mode": "RW"
        |    }]
        |  }
        |}
      """.
        stripMargin)

      val appDef = normalizeAndConvert(json.as[raml.App])
      appDef.residency should be(Some(Residency.defaultResidency))
      Done
    }
  }

  test("""FromJSON should parse "residency" """) {
    assumeValid {
      val appDef = normalizeAndConvert(Json.parse(
        """{
          |  "id": "test",
          |  "cmd": "foo",
          |  "container": {
          |    "type": "MESOS",
          |    "volumes": [{
          |      "containerPath": "var",
          |      "persistent": { "size": 10 },
          |      "mode": "RW"
          |    }]
          |  },
          |  "residency": {
          |    "relaunchEscalationTimeoutSeconds": 300,
          |    "taskLostBehavior": "RELAUNCH_AFTER_TIMEOUT"
          |  }
          |}""".stripMargin).as[raml.App])

      appDef.residency should equal(Some(Residency(300, Protos.ResidencyDefinition.TaskLostBehavior.RELAUNCH_AFTER_TIMEOUT)))
      Done
    }
  }

  test("ToJson should serialize residency") {
    import Fixture._

    val json = Json.toJson(a1.copy(residency = Some(Residency(7200, Protos.ResidencyDefinition.TaskLostBehavior.WAIT_FOREVER))))
    (json \ "residency" \ "relaunchEscalationTimeoutSeconds").as[Long] should equal(7200)
    (json \ "residency" \ "taskLostBehavior").as[String] should equal(Protos.ResidencyDefinition.TaskLostBehavior.WAIT_FOREVER.name())
  }

  test("AppDefinition JSON includes readinessChecks") {
    val app = AppDefinition(
      id = PathId("/test"),
      cmd = Some("sleep 123"), readinessChecks = Seq(
        ReadinessCheckTestHelper.alternativeHttps
      ),
      portDefinitions = Seq(
        state.PortDefinition(0, name = Some(ReadinessCheckTestHelper.alternativeHttps.portName))
      )
    )
    val appJson = Json.toJson(app)
    val rereadApp = appJson.as[raml.App]
    rereadApp.readinessChecks should have size 1
    assumeValid {
      normalizeAndConvert(rereadApp) should equal(app)
      Done
    }
  }

  test("FromJSON should parse ipAddress.networkName") {
    val appDef = normalizeAndConvert(Json.parse(
      """{
        |  "id": "test",
        |  "cmd": "foo",
        |  "ipAddress": {
        |    "networkName": "foo"
        |  }
        |}""".stripMargin).as[raml.App])

    appDef.networks should be(Seq(ContainerNetwork(name = "foo")))
  }

  test("FromJSON should parse ipAddress.networkName with MESOS container") {
    val appDef = normalizeAndConvert(Json.parse(
      """{
        |  "id": "test",
        |  "cmd": "foo",
        |  "ipAddress": {
        |    "networkName": "foo"
        |  },
        |  "container": {
        |    "type": "MESOS"
        |  }
        |}""".stripMargin).as[raml.App])

    appDef.networks should be(Seq(ContainerNetwork(name = "foo")))
    appDef.container should be(Some(Container.Mesos()))
  }

  test("FromJSON should parse ipAddress.networkName with DOCKER container w/o port mappings") {
    assumeValid {
      val appDef = normalizeAndConvert(Json.parse(
        """{
          |  "id": "test",
          |  "ipAddress": {
          |    "networkName": "foo"
          |  },
          |  "container": {
          |    "type": "DOCKER",
          |    "docker": {
          |      "image": "busybox",
          |      "network": "USER"
          |    }
          |  }
          |}""".stripMargin).as[raml.App])

      appDef.networks should be(Seq(ContainerNetwork(name = "foo")))
      appDef.container should be(Some(Container.Docker(image = "busybox")))
      Done
    }
  }

  test("FromJSON should parse ipAddress.networkName with DOCKER container w/ port mappings") {
    val appDef = normalizeAndConvert(Json.parse(
      """{
        |  "id": "test",
        |  "ipAddress": {
        |    "networkName": "foo"
        |  },
        |  "container": {
        |    "type": "DOCKER",
        |    "docker": {
        |      "image": "busybox",
        |      "network": "USER",
        |      "portMappings": [{
        |        "containerPort": 123, "servicePort": 80, "name": "foobar"
        |      }]
        |    }
        |  }
        |}""".stripMargin).as[raml.App])

    appDef.networks should be(Seq(ContainerNetwork(name = "foo")))
    appDef.container should be(Some(Container.Docker(
      image = "busybox",
      portMappings = Seq(
        Container.PortMapping(containerPort = 123, servicePort = 80, name = Some("foobar"))
      )
    )))
  }

  test("FromJSON should parse Mesos Docker container") {
    val appDef = normalizeAndConvert(Json.parse(
      """{
        |  "id": "test",
        |  "ipAddress": {
        |    "networkName": "foo"
        |  },
        |  "container": {
        |    "type": "MESOS",
        |    "docker": {
        |      "image": "busybox",
        |      "credential": {
        |        "principal": "aPrincipal",
        |        "secret": "aSecret"
        |      }
        |    }
        |  }
        |}""".stripMargin).as[raml.App])

    appDef.networks should be(Seq(ContainerNetwork(name = "foo")))
    appDef.container should be(Some(Container.MesosDocker(
      image = "busybox",
      credential = Some(Container.Credential("aPrincipal", Some("aSecret")))
    )))
  }

  test("FromJSON should parse Mesos AppC container") {
    val appDef = normalizeAndConvert(Json.parse(
      """{
        |  "id": "test",
        |  "ipAddress": {
        |    "networkName": "foo"
        |  },
        |  "container": {
        |    "type": "MESOS",
        |    "appc": {
        |      "image": "busybox",
        |      "id": "sha512-aHashValue",
        |      "labels": {
        |        "version": "1.2.0",
        |        "arch": "amd64",
        |        "os": "linux"
        |      }
        |    }
        |  }
        |}""".stripMargin).as[raml.App])

    appDef.networks should be(Seq(ContainerNetwork(name = "foo")))
    appDef.container should be(Some(Container.MesosAppC(
      image = "busybox",
      id = Some("sha512-aHashValue"),
      labels = Map(
        "version" -> "1.2.0",
        "arch" -> "amd64",
        "os" -> "linux"
      )
    )))
  }

  test("FromJSON should parse ipAddress without networkName") {
    val app = Json.parse(
      """{
        |  "id": "test",
        |  "ipAddress": { }
        |}""".stripMargin).as[raml.App]

    app.ipAddress.isDefined && app.ipAddress.get.networkName.isEmpty should equal(true)
  }

  test("FromJSON should parse secrets") {
    val app = Json.parse(
      """{
        |  "id": "test",
        |  "secrets": {
        |     "secret1": { "source": "/foo" },
        |     "secret2": { "source": "/foo" },
        |     "secret3": { "source": "/foo2" }
        |  }
        |}""".stripMargin).as[raml.App]

    app.secrets.keys.size should equal(3)
    app.secrets("secret1").source should equal("/foo")
    app.secrets("secret2").source should equal("/foo")
    app.secrets("secret3").source should equal("/foo2")
  }

  test("ToJSON should serialize secrets") {
    import Fixture._

    val json = Json.toJson(a1.copy(secrets = Map(
      "secret1" -> Secret("/foo"),
      "secret2" -> Secret("/foo"),
      "secret3" -> Secret("/foo2")
    )))
    (json \ "secrets" \ "secret1" \ "source").as[String] should equal("/foo")
    (json \ "secrets" \ "secret2" \ "source").as[String] should equal("/foo")
    (json \ "secrets" \ "secret3" \ "source").as[String] should equal("/foo2")
  }

  test("FromJSON should parse unreachable instance strategy") {
    val appDef = normalizeAndConvert(Json.parse(
      """{
        |  "id": "test",
        |  "unreachableStrategy": {
        |      "timeUntilInactiveSeconds": 600,
        |      "timeUntilExpungeSeconds": 1200
        |  }
        |}""".stripMargin).as[App])

    appDef.unreachableStrategy.timeUntilInactive should be(10.minutes)
    appDef.unreachableStrategy.timeUntilExpunge should be(20.minutes)
  }

  test("ToJSON should serialize unreachable instance strategy") {
    val strategy = UnreachableStrategy(6.minutes, 12.minutes)
    val appDef = AppDefinition(id = PathId("test"), unreachableStrategy = strategy)

    val json = Json.toJson(Raml.toRaml(appDef))

    (json \ "unreachableStrategy" \ "timeUntilInactiveSeconds").as[Long] should be(360)
    (json \ "unreachableStrategy" \ "timeUntilExpungeSeconds").as[Long] should be(720)
  }

  test("app with readinessCheck passes validation", Unstable) {
    val app = AppDefinition(
      id = "test".toRootPath,
      cmd = Some("sleep 1234"),
      readinessChecks = Seq(
        ReadinessCheckTestHelper.alternativeHttps
      ),
      portDefinitions = Seq(state.PortDefinition(0, name = Some(ReadinessCheckTestHelper.alternativeHttps.portName)))
    )

    val jsonApp = Json.toJson(Raml.toRaml(app))
    assumeValid {
      val ramlApp = jsonApp.as[raml.App]
      normalizeAndConvert(ramlApp)
      Done
    }
  }
}
