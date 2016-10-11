package mesosphere.marathon
package test

import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.core.instance.Instance.AgentInfo
import mesosphere.marathon.core.instance.update.InstanceUpdateOperation
import mesosphere.marathon.core.instance.{ Instance, LegacyAppInstance }
import mesosphere.marathon.core.launcher.impl.InstanceOpFactoryImpl
import mesosphere.marathon.core.launcher.{ InstanceOpFactory, OfferMatchResult }
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.state.{ AppDefinition, RunSpec }
import mesosphere.marathon.stream._
import mesosphere.marathon.tasks.PortsMatch
import mesosphere.mesos.ResourceMatcher.ResourceMatch
import mesosphere.mesos.TaskBuilder
import mesosphere.test.Clocked
import org.apache.mesos

class TestInstanceBuilder() extends Clocked {
  import TestInstanceBuilder._

  private[this] val opFactory = new InstanceOpFactoryImpl(config)(clock)

  /**
    * Build an instance based on the given app definition.
    */
  def buildFrom(app: AppDefinition): Instance = legacyInstance(app)

  def buildFrom(runSpec: RunSpec): Instance = {
    val request: InstanceOpFactory.Request = InstanceOpFactory.Request(
      runSpec, offer, Map.empty[Instance.Id, Instance], additionalLaunches = 1)
    val matchResult = opFactory.matchOfferRequest(request)
    matchResult match {
      case matches: OfferMatchResult.Match => extractInstance(matches.instanceOp.stateOp)
      case _ => throw new IllegalStateException(s"Matching offer request resulted in $matchResult")
    }
  }

  private[this] def extractInstance(op: InstanceUpdateOperation): Instance = op match {
    case InstanceUpdateOperation.LaunchEphemeral(instance) => instance
    case _ => throw new IllegalStateException(s"Extracting from $op not implemented")
  }
}

private object TestInstanceBuilder extends Clocked {
  val config: MarathonConf = MarathonTestHelper.defaultConfig()
  val attributes = Seq.empty[mesos.Protos.Attribute]
  val offer = mesos.Protos.Offer.newBuilder()
    .setHostname("hostname")
    .setSlaveId(mesos.Protos.SlaveID.newBuilder().setValue("agentId").build())
    .addAllAttributes(attributes)
    .setFrameworkId(mesos.Protos.FrameworkID.newBuilder().setValue("frameworkId"))
    .build()

  def legacyInstance(app: AppDefinition): Instance = {
    val resourceMatch: ResourceMatch = ResourceMatch(scalarMatches = Seq.empty, portsMatch = PortsMatch(Nil))
    val taskBuilder = new TaskBuilder(app, Task.Id.forRunSpec, config)
    val (taskInfo, networkInfo) = taskBuilder.build(offer, resourceMatch, volumeMatchOpt = None)
    val task = Task.LaunchedEphemeral(
      taskId = Task.Id(taskInfo.getTaskId),
      runSpecVersion = app.version,
      status = Task.Status(
        stagedAt = clock.now(),
        condition = Condition.Created,
        networkInfo = networkInfo
      )
    )

    val agentInfo = AgentInfo(offer)
    LegacyAppInstance(task, agentInfo, app.unreachableStrategy)
  }
}