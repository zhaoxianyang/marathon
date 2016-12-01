package mesosphere.marathon
package integration.setup

import mesosphere.marathon.core.event._
import mesosphere.marathon.raml.Raml
import mesosphere.marathon.state.{ Group, RootGroup, Timestamp }
import mesosphere.marathon.upgrade.DeploymentPlan
import org.apache.mesos.{ Protos => mesos }
import play.api.libs.functional.syntax._
import play.api.libs.json._

/**
  * Formats for JSON objects which do not need write support in the production code.
  */
object V2TestFormats {
  import mesosphere.marathon.api.v2.json.Formats._

  implicit lazy val GroupReads: Reads[Group] = Reads { js =>
    JsSuccess(Raml.fromRaml(js.as[raml.Group]))
  }

  implicit lazy val DeploymentPlanReads: Reads[DeploymentPlan] = Reads { js =>
    JsSuccess(
      DeploymentPlan(
        original = RootGroup.fromGroup((js \ "original").as[Group]),
        target = RootGroup.fromGroup((js \ "target").as[Group]),
        version = (js \ "version").as[Timestamp]).copy(id = (js \ "id").as[String]
        )
    )
  }

  implicit lazy val networkInfoProtocolReads = Reads[mesos.NetworkInfo.Protocol] { json =>
    val allowedProtocolString = mesos.NetworkInfo.Protocol.values().toSeq.map(
      _.getDescriptorForType.getName).mkString(", ")

    json.validate[String].flatMap { protocolString: String =>

      Option(mesos.NetworkInfo.Protocol.valueOf(protocolString)) match {
        case Some(protocol) => JsSuccess(protocol)
        case None =>
          JsError(s"'$protocolString' is not a valid protocol. Allowed values: $allowedProtocolString")
      }
    }
  }

  implicit lazy val ipAddressReads: Reads[mesos.NetworkInfo.IPAddress] = {

    def toIpAddress(ipAddress: String, protocol: mesos.NetworkInfo.Protocol): mesos.NetworkInfo.IPAddress =
      mesos.NetworkInfo.IPAddress.newBuilder().setIpAddress(ipAddress).setProtocol(protocol).build()

    (
      (__ \ "ipAddress").read[String] ~
      (__ \ "protocol").read[mesos.NetworkInfo.Protocol]
    )(toIpAddress _)
  }

  implicit lazy val SubscribeReads: Reads[Subscribe] = Json.reads[Subscribe]
  implicit lazy val UnsubscribeReads: Reads[Unsubscribe] = Json.reads[Unsubscribe]
  implicit lazy val EventStreamAttachedReads: Reads[EventStreamAttached] = Json.reads[EventStreamAttached]
  implicit lazy val EventStreamDetachedReads: Reads[EventStreamDetached] = Json.reads[EventStreamDetached]
  implicit lazy val RemoveHealthCheckReads: Reads[RemoveHealthCheck] = Json.reads[RemoveHealthCheck]
  implicit lazy val HealthStatusChangedReads: Reads[HealthStatusChanged] = Json.reads[HealthStatusChanged]
  implicit lazy val GroupChangeSuccessReads: Reads[GroupChangeSuccess] = Json.reads[GroupChangeSuccess]
  implicit lazy val GroupChangeFailedReads: Reads[GroupChangeFailed] = Json.reads[GroupChangeFailed]
  implicit lazy val DeploymentSuccessReads: Reads[DeploymentSuccess] = Json.reads[DeploymentSuccess]
  implicit lazy val DeploymentFailedReads: Reads[DeploymentFailed] = Json.reads[DeploymentFailed]
  implicit lazy val MesosStatusUpdateEventReads: Reads[MesosStatusUpdateEvent] = Json.reads[MesosStatusUpdateEvent]
  implicit lazy val MesosFrameworkMessageEventReads: Reads[MesosFrameworkMessageEvent] =
    Json.reads[MesosFrameworkMessageEvent]
  implicit lazy val SchedulerDisconnectedEventReads: Reads[SchedulerDisconnectedEvent] =
    Json.reads[SchedulerDisconnectedEvent]
  implicit lazy val SchedulerRegisteredEventWritesReads: Reads[SchedulerRegisteredEvent] =
    Json.reads[SchedulerRegisteredEvent]
  implicit lazy val SchedulerReregisteredEventWritesReads: Reads[SchedulerReregisteredEvent] =
    Json.reads[SchedulerReregisteredEvent]

  implicit lazy val eventSubscribersReads: Reads[EventSubscribers] = Reads { subscribersJson =>
    JsSuccess(EventSubscribers(urls = (subscribersJson \ "callbackUrls").asOpt[Set[String]].getOrElse(Set.empty)))
  }
}
