package mesosphere.marathon
package raml

import mesosphere.FunTest
import mesosphere.marathon.api.v2.AppNormalization
import mesosphere.marathon.api.v2.validation.AppValidation
import mesosphere.marathon.core.health.{ MarathonHttpHealthCheck, PortReference }
import mesosphere.marathon.core.pod.BridgeNetwork
import mesosphere.marathon.state.{ AppDefinition, BackoffStrategy, FetchUri, PathId }
import play.api.libs.json.Json
import org.apache.mesos.{ Protos => Mesos }

class AppConversionTest extends FunTest {

  test("An app is written to json and can be read again via formats") {
    Given("An app")
    val constraint = Protos.Constraint.newBuilder()
      .setField("foo")
      .setOperator(Protos.Constraint.Operator.CLUSTER)
      .setValue("1")
      .build()
    val app = AppDefinition(
      id = PathId("/test"),
      cmd = Some("test"),
      user = Some("user"),
      env = Map("A" -> state.EnvVarString("test")),
      instances = 23,
      resources = Resources(),
      executor = "executor",
      constraints = Set(constraint),
      fetch = Seq(FetchUri("http://test.this")),
      requirePorts = true,
      backoffStrategy = BackoffStrategy(),
      container = Some(state.Container.Docker(
        volumes = Seq(state.DockerVolume("/container", "/host", Mesos.Volume.Mode.RW)),
        image = "foo/bla",
        portMappings = Seq(state.Container.PortMapping(12, name = Some("http-api"), hostPort = Some(23), servicePort = 123)),
        privileged = true
      )),
      networks = Seq(BridgeNetwork()),
      healthChecks = Set(MarathonHttpHealthCheck(portIndex = Some(PortReference.ByIndex(0)))),
      readinessChecks = Seq(core.readiness.ReadinessCheck()),
      acceptedResourceRoles = Set("*")
    )

    When("The app is translated to json and read back from formats")
    val json = Json.toJson(app.toRaml[App])
    //filter out values that are written, but are not expected to read back
    import mesosphere.marathon.api.v2.Validation
    try {
      val readApp: AppDefinition = Raml.fromRaml(
        AppNormalization(
          Validation.validateOrThrow(
            AppNormalization.forDeprecatedFields(
              Validation.validateOrThrow(
                json.as[App]
              )(AppValidation.validateOldAppAPI)
            )
          )(AppValidation.validateCanonicalAppAPI(Set.empty)),
          AppNormalization.Config(None)
        )
      )
      Then("The app is identical")
      readApp should be(app)
    } catch {
      case vfe: ValidationFailedException =>
        assert(false, vfe.failure.violations)
    }
  }
}
