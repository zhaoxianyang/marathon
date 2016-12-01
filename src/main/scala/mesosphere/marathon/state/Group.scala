package mesosphere.marathon
package state

import java.util.Objects

import com.wix.accord._
import com.wix.accord.dsl._
import mesosphere.marathon.Protos.GroupDefinition
import mesosphere.marathon.api.v2.Validation._
import mesosphere.marathon.api.v2.validation.AppValidation
import mesosphere.marathon.core.pod.PodDefinition
import mesosphere.marathon.plugin.{ Group => IGroup }
import mesosphere.marathon.state.Group._
import mesosphere.marathon.state.PathId._
import mesosphere.marathon.stream._

class Group(
    val id: PathId,
    val apps: Map[AppDefinition.AppKey, AppDefinition] = defaultApps,
    val pods: Map[PathId, PodDefinition] = defaultPods,
    val groupsById: Map[Group.GroupKey, Group] = defaultGroups,
    val dependencies: Set[PathId] = defaultDependencies,
    val version: Timestamp = defaultVersion,
    val transitiveAppsById: Map[AppDefinition.AppKey, AppDefinition],
    val transitivePodsById: Map[PathId, PodDefinition]) extends MarathonState[GroupDefinition, Group] with IGroup {
  override def mergeFromProto(msg: GroupDefinition): Group = Group.fromProto(msg)
  override def mergeFromProto(bytes: Array[Byte]): Group = Group.fromProto(GroupDefinition.parseFrom(bytes))

  override def toProto: GroupDefinition = {
    GroupDefinition.newBuilder
      .setId(id.toString)
      .setVersion(version.toString)
      .addAllDeprecatedApps(apps.map { case (_, app) => app.toProto })
      .addAllGroups(groupsById.map { case (_, group) => group.toProto })
      .addAllDependencies(dependencies.map(_.toString))
      .build()
  }

  def app(appId: PathId): Option[AppDefinition] = transitiveAppsById.get(appId)
  def pod(podId: PathId): Option[PodDefinition] = transitivePodsById.get(podId)
  def group(groupId: PathId): Option[Group] = transitiveGroupsById.get(groupId)

  lazy val transitiveApps: Set[AppDefinition] = transitiveAppsById.values.toSet
  lazy val transitiveAppIds: Set[PathId] = transitiveAppsById.keySet

  lazy val transitiveRunSpecsById: Map[PathId, RunSpec] = transitiveAppsById ++ transitivePodsById
  lazy val transitiveRunSpecs: Set[RunSpec] = transitiveRunSpecsById.values.toSet

  lazy val transitiveGroupsById: Map[Group.GroupKey, Group] = {
    Map(id -> this) ++ groupsById.values.flatMap(_.transitiveGroupsById)
  }

  /** @return true if and only if this group directly or indirectly contains app definitions. */
  def containsApps: Boolean = apps.nonEmpty || groupsById.exists { case (_, group) => group.containsApps }

  def containsPods: Boolean = pods.nonEmpty || groupsById.exists { case (_, group) => group.containsPods }

  def containsAppsOrPodsOrGroups: Boolean = apps.nonEmpty || groupsById.nonEmpty || pods.nonEmpty

  override def equals(other: Any): Boolean = other match {
    case that: Group =>
      id == that.id &&
        apps == that.apps &&
        pods == that.pods &&
        groupsById == that.groupsById &&
        dependencies == that.dependencies &&
        version == that.version
    case _ => false
  }

  override def hashCode(): Int = Objects.hash(id, apps, pods, groupsById, dependencies, version)

  override def toString = s"Group($id, ${apps.values}, ${pods.values}, ${groupsById.values}, $dependencies, $version)"
}

object Group {
  type GroupKey = PathId

  def apply(
    id: PathId,
    apps: Map[AppDefinition.AppKey, AppDefinition] = Group.defaultApps,
    pods: Map[PathId, PodDefinition] = Group.defaultPods,
    groupsById: Map[Group.GroupKey, Group] = Group.defaultGroups,
    dependencies: Set[PathId] = Group.defaultDependencies,
    version: Timestamp = Group.defaultVersion,
    transitiveAppsById: Map[AppDefinition.AppKey, AppDefinition],
    transitivePodsById: Map[PathId, PodDefinition]): Group =
    new Group(id, apps, pods, groupsById, dependencies, version, transitiveAppsById, transitivePodsById)

  def empty(id: PathId): Group =
    Group(id = id, version = Timestamp(0), transitiveAppsById = Map.empty, transitivePodsById = Map.empty)

  def fromProto(msg: GroupDefinition): Group = {
    val appsById: Map[AppDefinition.AppKey, AppDefinition] = msg.getDeprecatedAppsList.map { proto =>
      val app = AppDefinition.fromProto(proto)
      app.id -> app
    }(collection.breakOut)
    val podsById: Map[PathId, PodDefinition] = msg.getDeprecatedPodsList.map { proto =>
      val pod = PodDefinition.fromProto(proto)
      pod.id -> pod
    }(collection.breakOut)
    val groupsById: Map[PathId, Group] = msg.getGroupsList.map(fromProto).map(group => group.id -> group)(collection.breakOut)
    Group(
      id = msg.getId.toPath,
      apps = appsById,
      pods = podsById,
      groupsById = groupsById,
      dependencies = msg.getDependenciesList.map(PathId.apply)(collection.breakOut),
      version = Timestamp(msg.getVersion),
      transitiveAppsById = appsById ++ groupsById.values.flatMap(_.transitiveAppsById),
      transitivePodsById = podsById ++ groupsById.values.flatMap(_.transitivePodsById))
  }

  def defaultApps: Map[AppDefinition.AppKey, AppDefinition] = Map.empty
  val defaultPods = Map.empty[PathId, PodDefinition]
  def defaultGroups: Map[Group.GroupKey, Group] = Map.empty
  def defaultDependencies: Set[PathId] = Set.empty
  def defaultVersion: Timestamp = Timestamp.now()

  def validGroup(base: PathId, enabledFeatures: Set[String]): Validator[Group] =
    validator[Group] { group =>
      group.id is validPathWithBase(base)
      group.apps.values as "apps" is every(
        AppDefinition.validNestedAppDefinition(group.id.canonicalPath(base), enabledFeatures))
      group is noAppsAndPodsWithSameId
      group is noAppsAndGroupsWithSameName
      group is noPodsAndGroupsWithSameName
      group.groupsById.values as "groups" is every(validGroup(group.id.canonicalPath(base), enabledFeatures))
    }

  private def noAppsAndPodsWithSameId: Validator[Group] =
    isTrue("Applications and Pods may not share the same id") { group =>
      val podIds = group.transitivePodsById.keySet
      val appIds = group.transitiveAppsById.keySet
      appIds.intersect(podIds).isEmpty
    }

  private def noAppsAndGroupsWithSameName: Validator[Group] =
    isTrue("Groups and Applications may not have the same identifier.") { group =>
      val groupIds = group.groupsById.keySet
      val clashingIds = groupIds.intersect(group.apps.keySet)
      clashingIds.isEmpty
    }

  private def noPodsAndGroupsWithSameName: Validator[Group] =
    isTrue("Groups and Pods may not have the same identifier.") { group =>
      val groupIds = group.groupsById.keySet
      val clashingIds = groupIds.intersect(group.pods.keySet)
      clashingIds.isEmpty
    }

  def emptyUpdate(id: PathId): raml.GroupUpdate = raml.GroupUpdate(Some(id.toString))

  /** requires that apps are in canonical form */
  def validNestedGroupUpdateWithBase(base: PathId, enabledFeatures: Set[String]): Validator[raml.GroupUpdate] =
    validator[raml.GroupUpdate] { group =>
      group is notNull

      group.version is theOnlyDefinedOptionIn(group)
      group.scaleBy is theOnlyDefinedOptionIn(group)

      group.id.map(_.toPath) as "id" is optional(valid)
      group.apps is optional(every(
        AppValidation.validNestedApp(group.id.fold(base)(PathId(_).canonicalPath(base)), enabledFeatures)))
      group.groups is optional(every(
        validNestedGroupUpdateWithBase(group.id.fold(base)(PathId(_).canonicalPath(base)), enabledFeatures)))
    }

  def groupUpdateValid(enabledFeatures: Set[String]): Validator[raml.GroupUpdate] =
    validNestedGroupUpdateWithBase(PathId.empty, enabledFeatures)
}
