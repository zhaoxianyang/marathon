package mesosphere.marathon
package raml

import mesosphere.marathon.core.pod.{ EphemeralVolume, HostVolume, Volume => PodVolume }
import mesosphere.marathon.state.{ DiskType, ExternalVolumeInfo, PersistentVolumeInfo }
import org.apache.mesos.{ Protos => Mesos }

trait VolumeConversion extends ConstraintConversion with DefaultConversions {

  implicit val volumeRamlReader: Reads[Volume, PodVolume] = Reads { v =>
    v.host match {
      case Some(hostPath) => HostVolume(v.name, hostPath)
      case None => EphemeralVolume(v.name)
    }
  }

  implicit val volumeRamlWriter: Writes[PodVolume, Volume] = Writes {
    case e: EphemeralVolume => Volume(e.name)
    case h: HostVolume => Volume(h.name, Some(h.hostPath))
  }

  implicit val volumeModeWrites: Writes[Mesos.Volume.Mode, ReadMode] = Writes {
    case Mesos.Volume.Mode.RO => ReadMode.Ro
    case Mesos.Volume.Mode.RW => ReadMode.Rw
  }

  implicit val volumeModeReads: Reads[ReadMode, Mesos.Volume.Mode] = Reads {
    case ReadMode.Ro => Mesos.Volume.Mode.RO
    case ReadMode.Rw => Mesos.Volume.Mode.RW
  }

  implicit val volumeWrites: Writes[state.Volume, AppVolume] = Writes { volume =>

    implicit val externalVolumeWrites: Writes[state.ExternalVolumeInfo, ExternalVolume] = Writes { ev =>
      ExternalVolume(size = ev.size, name = Some(ev.name), provider = Some(ev.provider), options = ev.options)
    }

    implicit val persistentVolumeInfoWrites: Writes[state.PersistentVolumeInfo, PersistentVolume] = Writes { pv =>
      val pvType = pv.`type` match {
        case DiskType.Mount => PersistentVolumeType.Mount
        case DiskType.Path => PersistentVolumeType.Path
        case DiskType.Root => PersistentVolumeType.Root
      }
      PersistentVolume(Some(pvType), pv.size, pv.maxSize, pv.constraints.toRaml[Seq[Seq[String]]])
    }

    def create(hostPath: Option[String] = None, persistent: Option[PersistentVolume] = None, external: Option[ExternalVolume] = None): AppVolume = AppVolume(
      containerPath = volume.containerPath,
      hostPath = hostPath,
      persistent = persistent,
      external = external,
      mode = volume.mode.toRaml
    )

    volume match {
      case dv: state.DockerVolume => create(Some(dv.hostPath))
      case ev: state.ExternalVolume => create(external = Some(ev.external.toRaml))
      case pv: state.PersistentVolume => create(persistent = Some(pv.persistent.toRaml))
    }
  }

  implicit val appVolumeRamlReader: Reads[AppVolume, state.Volume] = Reads { vol =>
    def failed[T](msg: String): T =
      throw new SerializationFailedException(msg)

    val result: state.Volume = vol match {
      case AppVolume(ctPath, hostPath, None, Some(external), mode) =>
        val info = Some(ExternalVolumeInfo(
          size = external.size,
          name = external.name.getOrElse(failed("external volume requires a name")),
          provider = external.provider.getOrElse(failed("external volume requires a provider")),
          options = external.options
        ))
        state.Volume(containerPath = ctPath, hostPath = hostPath, mode = mode.fromRaml, persistent = None, external = info)
      case AppVolume(ctPath, hostPath, Some(persistent), None, mode) =>
        val volType = persistent.`type` match {
          case Some(definedType) => definedType match {
            case PersistentVolumeType.Root => DiskType.Root
            case PersistentVolumeType.Mount => DiskType.Mount
            case PersistentVolumeType.Path => DiskType.Path
          }
          case None => DiskType.Root
        }
        val info = Some(PersistentVolumeInfo(
          size = persistent.size,
          maxSize = persistent.maxSize,
          `type` = volType,
          constraints = persistent.constraints.map { constraint =>
            (constraint.headOption, constraint.lift(1), constraint.lift(2)) match {
              case (Some("path"), Some("LIKE"), Some(value)) =>
                Protos.Constraint.newBuilder()
                  .setField("path")
                  .setOperator(Protos.Constraint.Operator.LIKE)
                  .setValue(value)
                  .build()
              case _ =>
                throw SerializationFailedException(s"illegal volume constraint ${constraint.mkString(",")}")
            }
          }(collection.breakOut)
        ))
        state.Volume(containerPath = ctPath, hostPath = hostPath, mode = mode.fromRaml, persistent = info, external = None)
      case AppVolume(ctPath, hostPath, None, None, mode) =>
        state.Volume(containerPath = ctPath, hostPath = hostPath, mode = mode.fromRaml, persistent = None, external = None)
      case v => failed(s"illegal volume specification $v")
    }
    result
  }
}

object VolumeConversion extends VolumeConversion
