package mesosphere.marathon
package raml

import mesosphere.marathon.state
import scala.concurrent.duration._

/**
  * Conversion from [[mesosphere.marathon.state.UnreachableStrategy]] to [[mesosphere.marathon.raml.UnreachableStrategy]].
  */
trait UnreachableStrategyConversion {

  implicit val ramlRead = Reads[UnreachableStrategy, state.UnreachableStrategy] { handling =>
    state.UnreachableStrategy(
      timeUntilInactiveSeconds = handling.timeUntilInactiveSeconds.seconds,
      timeUntilExpungeSeconds = handling.timeUntilExpungeSeconds.seconds)
  }

  implicit val ramlWrite = Writes[state.UnreachableStrategy, UnreachableStrategy]{ handling =>
    UnreachableStrategy(
      timeUntilInactiveSeconds = handling.timeUntilInactiveSeconds.toSeconds,
      timeUntilExpungeSeconds = handling.timeUntilExpungeSeconds.toSeconds)
  }
}

object UnreachableStrategyConversion extends UnreachableStrategyConversion
