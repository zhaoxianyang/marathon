package mesosphere.marathon
package raml

import mesosphere.marathon.state
import scala.concurrent.duration._

/**
  * Conversion from [[mesosphere.marathon.state.UnreachableInstanceHandling]] to [[mesosphere.marathon.raml.UnreachableInstanceHandling]].
  */
trait UnreachableInstanceHandlingConversion {

  implicit val ramlRead = Reads[UnreachableInstanceHandling, state.UnreachableInstanceHandling] { handling =>
    state.UnreachableInstanceHandling(
      timeUntilInactive = handling.timeUntilInactive.seconds,
      timeUntilExpunge = handling.timeUntilExpunge.seconds)
  }

  implicit val ramlWrite = Writes[state.UnreachableInstanceHandling, UnreachableInstanceHandling]{ handling =>
    UnreachableInstanceHandling(
      timeUntilInactive = handling.timeUntilInactive.toSeconds,
      timeUntilExpunge = handling.timeUntilExpunge.toSeconds)
  }
}

object UnreachableInstanceHandlingConversion extends UnreachableInstanceHandlingConversion
