package mesosphere.mesos

import mesosphere.marathon.state.{ DiskSource, PersistentVolume }
import mesosphere.mesos.protos.Resource
import org.apache.mesos.Protos
import org.apache.mesos.Protos.Resource.DiskInfo.Source

import scala.collection.immutable.Seq

// TODO - put this somewhere sensible
object ResourceHelpers {

  def requestedStringification(requested: Either[Double, PersistentVolume]): String = requested match {
    case Left(value) => s"disk:root:$value"
    case Right(vol) =>
      import mesosphere.marathon.raml._
      val constraintsJson: Seq[Seq[String]] = vol.persistent.constraints.map(_.toRaml[Seq[String]])(collection.breakOut)
      s"disk:${vol.persistent.`type`.toString}:${vol.persistent.size}:[${constraintsJson.mkString(",")}]"
  }

  implicit class DiskRichResource(resource: Protos.Resource) {
    def getSourceOption: Option[Source] =
      if (resource.hasDisk && resource.getDisk.hasSource)
        Some(resource.getDisk.getSource)
      else
        None

    def getStringification: String = {
      require(resource.getName == Resource.DISK)
      val diskSource = DiskSource.fromMesos(getSourceOption)
      /* TODO - make this match mesos stringification */
      (List(
        resource.getName,
        diskSource.diskType.toString,
        resource.getScalar.getValue.toString) ++
        diskSource.path).mkString(":")
    }

    def afterAllocation(amount: Double): Option[Protos.Resource] = {
      val isMountDiskResource: Boolean =
        resource.hasDisk && resource.getDisk.hasSource &&
          (resource.getDisk.getSource.getType == Source.Type.MOUNT)

      if (isMountDiskResource || amount >= resource.getScalar.getValue)
        None
      else
        Some(
          resource.toBuilder.
          setScalar(
            Protos.Value.Scalar.newBuilder.
              setValue(resource.getScalar.getValue - amount)).
            build)
    }
  }
}
