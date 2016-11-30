package mesosphere.marathon
package core.task.jobs.impl

import akka.actor.{ Actor, ActorLogging, Cancellable, Props }
import akka.event.LoggingAdapter
import akka.pattern.pipe

import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.update.InstanceUpdateOperation
import mesosphere.marathon.core.task.jobs.TaskJobsConfig
import mesosphere.marathon.core.task.tracker.{ InstanceTracker, TaskStateOpProcessor }
import mesosphere.marathon.core.task.tracker.InstanceTracker.SpecInstances
import mesosphere.marathon.state.{ PathId, Timestamp }

/**
  * Business logic of overdue tasks actor.
  *
  * Factoring out into a trait makes testing simpler.
  */
trait ExpungeOverdueLostTasksActorLogic {

  def log: LoggingAdapter
  val config: TaskJobsConfig
  val clock: Clock
  val stateOpProcessor: TaskStateOpProcessor

  def triggerExpunge(instance: Instance): Unit = {
    val since = instance.state.since
    log.warning(s"Instance ${instance.instanceId} is unreachable since $since and will be expunged.")
    val stateOp = InstanceUpdateOperation.ForceExpunge(instance.instanceId)
    stateOpProcessor.process(stateOp)
  }

  /**
    * @return instances that have been UnreachableInactive for more than half of [[mesosphere.marathon.core.task.jobs.TaskJobsConfig.taskLostExpungeGC]] millis.
    */
  def filterOverdueUnreachableInactive(instances: Map[PathId, SpecInstances], now: Timestamp) =
    instances.values.flatMap(_.instances)
      .withFilter(_.isUnreachableInactive)
      .withFilter { instance =>
        val unreachableExpungeAfter = instance.unreachableStrategy.unreachableExpungeAfter
        instance.tasksMap.valuesIterator.exists(_.isUnreachableExpired(now, unreachableExpungeAfter))
      }
}

class ExpungeOverdueLostTasksActor(
    val clock: Clock,
    val config: TaskJobsConfig,
    instanceTracker: InstanceTracker,
    val stateOpProcessor: TaskStateOpProcessor) extends Actor with ActorLogging with ExpungeOverdueLostTasksActorLogic {

  import ExpungeOverdueLostTasksActor._
  implicit val ec = context.dispatcher

  var tickTimer: Option[Cancellable] = None

  override def preStart(): Unit = {
    log.info("ExpungeOverdueLostTasksActor has started")
    tickTimer = Some(context.system.scheduler.schedule(
      config.taskLostExpungeInitialDelay,
      config.taskLostExpungeInterval, self, Tick))
  }

  override def postStop(): Unit = {
    tickTimer.foreach(_.cancel())
    log.info("ExpungeOverdueLostTasksActor has stopped")
  }

  override def receive: Receive = {
    case Tick => instanceTracker.instancesBySpec() pipeTo self
    case InstanceTracker.InstancesBySpec(instances) =>
      filterOverdueUnreachableInactive(instances, clock.now()).foreach(triggerExpunge)
  }
}

object ExpungeOverdueLostTasksActor {

  case object Tick

  def props(clock: Clock, config: TaskJobsConfig,
    instanceTracker: InstanceTracker, stateOpProcessor: TaskStateOpProcessor): Props = {
    Props(new ExpungeOverdueLostTasksActor(clock, config, instanceTracker, stateOpProcessor))
  }
}
