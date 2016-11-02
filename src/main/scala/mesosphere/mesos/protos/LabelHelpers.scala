package mesosphere.mesos.protos

import mesosphere.marathon.stream._
import org.apache.mesos.Protos.{ Label, Labels }

trait LabelHelpers {

  implicit final class MesosLabels(labels: Map[String, String]) {
    def toMesosLabels: Labels =
      Labels.newBuilder().addAllLabels(labels.map(e => Label.newBuilder.setKey(e._1).setValue(e._2).build)).build
  }
}

object LabelHelpers extends LabelHelpers