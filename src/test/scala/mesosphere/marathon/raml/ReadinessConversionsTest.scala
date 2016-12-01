package mesosphere.marathon
package raml

import mesosphere.UnitTest
import mesosphere.marathon.core.readiness.{ ReadinessCheck => CoreReadinessCheck }

import scala.concurrent.duration._

class ReadinessConversionsTest extends UnitTest {

  "core readiness checks" when {

    val check = CoreReadinessCheck()

    "converted to RAML" should {

      val raml = check.toRaml[ReadinessCheck]

      "convert all fields to RAML" in {
        raml.httpStatusCodesForReady should contain theSameElementsAs check.httpStatusCodesForReady
        raml.intervalSeconds should be(check.interval.toSeconds)
        raml.name should be(check.name)
        raml.path should be(check.path)
        raml.portName should be(check.portName)
        raml.preserveLastResponse should be(check.preserveLastResponse)
        raml.timeoutSeconds should be(check.timeout.toSeconds)
        raml.protocol should be(check.protocol.toRaml[HttpScheme])
      }
    }
  }

  "RAML readiness checks" when {

    val check = ReadinessCheck()

    "converted to core ReadinessCheck" should {

      val coreCheck: CoreReadinessCheck = check.fromRaml

      "convert all fields to their core equivalent" in {
        coreCheck.httpStatusCodesForReady should be(check.httpStatusCodesForReady)
        coreCheck.interval should be(check.intervalSeconds.seconds)
        coreCheck.name should be(check.name)
        coreCheck.path should be(check.path)
        coreCheck.portName should be(check.portName)
        coreCheck.preserveLastResponse should be(check.preserveLastResponse)
        coreCheck.timeout should be(check.timeoutSeconds.seconds)
        coreCheck.protocol should be(check.protocol.fromRaml[CoreReadinessCheck.Protocol])
      }
    }
  }
}
