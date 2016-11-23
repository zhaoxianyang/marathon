package mesosphere.marathon.state

import scala.concurrent.duration._

/**
  * Defines the time outs and kill strategy for unreachable tasks.
  */
case class UnreachableStrategy(timeUntilInactive: FiniteDuration = 3.minutes, timeUntilExpunge: FiniteDuration = 6.minutes)
