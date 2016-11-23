package mesosphere.marathon.state

import scala.concurrent.duration._

/**
  * Defines the time outs and kill strategy for unreachable tasks.
  */
case class UnreachableStrategy(timeUntilInactiveSeconds: FiniteDuration = 3.minutes, timeUntilExpungeSeconds: FiniteDuration = 6.minutes)
