package mesosphere.marathon
package state

import scala.concurrent.duration._

/**
  * Defines the time outs for unreachable tasks.
  */
case class UnreachableStrategy(
  unreachableInactiveAfter: FiniteDuration = UnreachableStrategy.DefaultTimeUntilInactive,
  unreachableExpungeAfter: FiniteDuration = UnreachableStrategy.DefaultTimeUntilExpunge)

object UnreachableStrategy {
  val DefaultTimeUntilInactive = 3.minutes
  val DefaultTimeUntilExpunge = 6.minutes
}
