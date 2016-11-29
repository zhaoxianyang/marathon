package mesosphere.marathon
package state

import mesosphere.UnitTest
import mesosphere.marathon.state.UnreachableStrategy.KillSelection.{ OldestFirst, YoungestFirst }

import scala.concurrent.duration._

class UnreachableStrategyTest extends UnitTest {

  "UnreachableStrategy.KillSelection" should {

    "parse all value 'YoungestFirst'" in {
      UnreachableStrategy.KillSelection.withName("YoungestFirst") should be(YoungestFirst)
    }

    "parse all value 'OldestFirst'" in {
      UnreachableStrategy.KillSelection.withName("OldestFirst") should be(OldestFirst)
    }
  }

  it should {
    "throw an exception for an invalid value" in {
      the[NoSuchElementException] thrownBy {
        UnreachableStrategy.KillSelection.withName("youngestFirst")
      } should have message ("There is no KillSelection with name 'youngestFirst'")
    }
  }

  "UnreachableStrategy.YoungestFirst" should {
    "select the younger timestamp" in {
      YoungestFirst(Timestamp.zero, Timestamp(1)) should be(false)
      YoungestFirst(Timestamp(1), Timestamp.zero) should be(true)
    }
  }

  "UnreachableStrategy.OldestFirst" should {
    "select the older timestamp" in {
      OldestFirst(Timestamp.zero, Timestamp(1)) should be(true)
      OldestFirst(Timestamp(1), Timestamp.zero) should be(false)
    }
  }

  "UnreachableStrategy.unreachableStrategyValidator" should {
    "validate default strategy" in {
      val strategy = UnreachableStrategy()
      UnreachableStrategy.unreachableStrategyValidator(strategy).isSuccess should be(true)
    }

    "fail with invalid time until inactive" in {
      val strategy = UnreachableStrategy(timeUntilInactive = 0.seconds)
      UnreachableStrategy.unreachableStrategyValidator(strategy).isSuccess should be(false)
    }

    "fail when time until expunge is smaller" in {
      val strategy = UnreachableStrategy(timeUntilInactive = 2.seconds, timeUntilExpunge = 1.second)
      UnreachableStrategy.unreachableStrategyValidator(strategy).isSuccess should be(false)
    }
  }
}
