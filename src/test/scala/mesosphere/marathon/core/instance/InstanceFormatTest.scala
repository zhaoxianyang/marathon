package mesosphere.marathon
package core.instance

import mesosphere.UnitTest
import mesosphere.marathon.state.UnreachableStrategy
import play.api.libs.json._

import scala.concurrent.duration._

class InstanceFormatTest extends UnitTest {

  import Instance._

  "Instance.KillSelectionFormat" should {
    "create a proper JSON object from YoungestFirst" in {
      val json = Json.toJson(UnreachableStrategy.YoungestFirst)
      json.as[String] should be("YoungestFirst")
    }

    "create a proper JSON object from OldestFirst" in {
      val json = Json.toJson(UnreachableStrategy.OldestFirst)
      json.as[String] should be("OldestFirst")
    }
  }

  "Instance.UnreachableStrategyFormat" should {
    "parse a proper JSON" in {
      val json = Json.parse("""{ "timeUntilInactive": 1, "timeUntilExpunge": 2, "killSelection": "YoungestFirst" }""")
      json.as[UnreachableStrategy].killSelection should be(UnreachableStrategy.YoungestFirst)
      json.as[UnreachableStrategy].timeUntilInactive should be(1.second)
      json.as[UnreachableStrategy].timeUntilExpunge should be(2.seconds)
    }

    "parse a JSON with empty fields" in {
      val json = Json.parse("""{ "timeUntilExpunge": 2 }""")
      json.as[UnreachableStrategy].killSelection should be(UnreachableStrategy.DefaultKillSelection)
      json.as[UnreachableStrategy].timeUntilInactive should be(UnreachableStrategy.DefaultTimeUntilInactive)
      json.as[UnreachableStrategy].timeUntilExpunge should be(2.seconds)
    }

    "fail on an invalid kill selection" in {
      val json = Json.parse("""{ "timeUntilInactive": 1, "timeUntilExpunge": 2, "killSelection": "youngestFirst" }""")
      // Still fails because Reads.readNullable falls back to default.
      the [JsResultException] thrownBy {
        json.as[UnreachableStrategy]
      } should have message("JsResultException(errors:List((/killSelection,List(ValidationError(List(There is no KillSelection with name 'youngestFirst'),WrappedArray())))))")
    }
  }
}
