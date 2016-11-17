package mesosphere.marathon
package raml

import mesosphere.marathon.state.AppDefinition

trait AppConversion {

  // FIXME: implement complete conversion for all app fields
  // See https://mesosphere.atlassian.net/browse/MARATHON-1291
  implicit val appWriter: Writes[AppDefinition, App] = Writes { app =>
    App(id = app.id.toString)
  }
}
