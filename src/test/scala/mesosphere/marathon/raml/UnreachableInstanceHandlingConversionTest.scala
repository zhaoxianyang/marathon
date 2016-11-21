package mesosphere.marathon
package raml

import mesosphere.marathon.state
import mesosphere.UnitTest

import scala.concurrent.duration._

class UnreachableInstanceHandlingConversionTest extends UnitTest {

  "UnreachableInactiveHandlingConversion" should {
    "read from RAML" in {
      val raml = UnreachableInstanceHandling()

      val result: state.UnreachableInstanceHandling = UnreachableInstanceHandlingConversion.ramlRead(raml)

      result.timeUntilInactive should be(5.minutes)
      result.timeUntilExpunge should be(10.minutes)
    }
  }

  it should {
    "write to RAML" in {
      val handling = state.UnreachableInstanceHandling(10.minutes, 20.minutes)

      val raml: UnreachableInstanceHandling = UnreachableInstanceHandlingConversion.ramlWrite(handling)

      raml.timeUntilInactive should be(600)
      raml.timeUntilExpunge should be(1200)
    }
  }
}
