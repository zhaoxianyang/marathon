package mesosphere.marathon.api.v2.json

import mesosphere.marathon.ValidationFailedException
import mesosphere.marathon.api.v2.AppNormalization
import mesosphere.marathon.api.v2.validation.AppValidation
import mesosphere.marathon.raml.AppUpdate
import mesosphere.marathon.state.ResourceRole
import mesosphere.marathon.test.MarathonSpec
import org.scalatest.Matchers
import play.api.libs.json.Json

class AppUpdateFormatTest extends MarathonSpec with Matchers {

  def normalizedAndValidated(appUpdate: AppUpdate): AppUpdate = {
    import mesosphere.marathon.api.v2.Validation
    Validation.validateOrThrow(appUpdate)(AppValidation.validateOldAppUpdateAPI)
    val n1 = AppNormalization.forDeprecatedFields(appUpdate)
    Validation.validateOrThrow(n1)(AppValidation.validateCanonicalAppUpdateAPI(Set.empty))
    AppNormalization(n1, AppNormalization.Config(None))
  }

  def fromJson(json: String): AppUpdate =
    normalizedAndValidated(Json.parse(json).as[AppUpdate])

  test("should fail if id is /") {
    val json = """{"id": "/"}"""
    a[ValidationFailedException] shouldBe thrownBy { fromJson(json) }
  }

  test("FromJSON should not fail when 'cpus' is greater than 0") {
    val json = """ { "id": "test", "cpus": 0.0001 }"""
    noException should be thrownBy { fromJson(json) }
  }

  test("""FromJSON should parse "acceptedResourceRoles": ["production", "*"] """) {
    val json = """ { "id": "test", "acceptedResourceRoles": ["production", "*"] }"""
    val appUpdate = fromJson(json)
    appUpdate.acceptedResourceRoles should equal(Some(Set("production", ResourceRole.Unreserved)))
  }

  test("""FromJSON should parse "acceptedResourceRoles": ["*"] """) {
    val json = """ { "id": "test", "acceptedResourceRoles": ["*"] }"""
    val appUpdate = fromJson(json)
    appUpdate.acceptedResourceRoles should equal(Some(Set(ResourceRole.Unreserved)))
  }

  test("FromJSON should fail when 'acceptedResourceRoles' is defined but empty") {
    val json = """ { "id": "test", "acceptedResourceRoles": [] }"""
    a[ValidationFailedException] shouldBe thrownBy { fromJson(json) }
  }

  // Regression test for #3140
  test("FromJSON should set healthCheck portIndex to 0 when neither port nor portIndex are set") {
    val json = """ { "id": "test", "healthChecks": [{ "path": "/", "protocol": "HTTP" }] } """
    val appUpdate = fromJson(json)
    appUpdate.healthChecks.get.head.portIndex should equal(Some(0))
  }
}
