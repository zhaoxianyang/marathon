package mesosphere.marathon
package upgrade

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.event.EventStream
import akka.stream.{ActorMaterializer, Materializer}
import mesosphere.marathon.MarathonSchedulerActor.{CommandFailed, DeploymentStarted, RetrieveRunningDeployments, RunningDeployments}
import mesosphere.marathon.core.health.HealthCheckManager
import mesosphere.marathon.core.launchqueue.LaunchQueue
import mesosphere.marathon.core.readiness.{ReadinessCheckExecutor, ReadinessCheckResult}
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.termination.KillService
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.io.storage.StorageProvider
import mesosphere.marathon.state.PathId
import mesosphere.marathon.storage.repository.DeploymentRepository
import mesosphere.marathon.stream.Sink
import mesosphere.marathon.upgrade.DeploymentActor.Cancel
import org.apache.mesos.SchedulerDriver
import org.slf4j.LoggerFactory

import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal
import scala.async.Async.{async, await}
import scala.util.{Failure, Success}

class DeploymentManager(
    taskTracker: InstanceTracker,
    killService: KillService,
    launchQueue: LaunchQueue,
    scheduler: SchedulerActions,
    storage: StorageProvider,
    healthCheckManager: HealthCheckManager,
    eventBus: EventStream,
    readinessCheckExecutor: ReadinessCheckExecutor,
    marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
    deploymentRepository: DeploymentRepository) extends Actor with ActorLogging {
  import context.dispatcher
  import mesosphere.marathon.upgrade.DeploymentManager._

  private[this] val log = LoggerFactory.getLogger(getClass)

  val runningDeployments: mutable.Map[String, DeploymentInfo] =
    mutable.Map.empty.withDefaultValue(DeploymentInfo(None, DeploymentPlan.empty, Scheduled, Seq.empty))
  val deploymentStatus: mutable.Map[String, DeploymentStepInfo] = mutable.Map.empty

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case NonFatal(e) => Stop
  }

  @SuppressWarnings(Array("OptionGet"))
  def driver: SchedulerDriver = marathonSchedulerDriverHolder.driver.get

  def receive: Receive = {

    case StopAllDeployments =>
      for ((_, DeploymentInfo(Some(ref), _, _, _)) <- runningDeployments)
        ref ! DeploymentActor.Shutdown
      runningDeployments.clear()
      deploymentStatus.clear()

    case CancelDeployment(id) =>
      runningDeployments.get(id) match {
        case Some(DeploymentInfo(_, _, Scheduled, _)) =>
          runningDeployments.remove(id)
        case Some(DeploymentInfo(Some(ref), _, Deploying, _)) =>
          ref ! Cancel(new DeploymentCanceledException("The upgrade has been cancelled"))
        case Some(DeploymentInfo(_, _, Canceling, _)) =>
          log.warn(s"The deployment $id is already being canceled.")
        // This means we have a deployment with a [Deploying] status which has no DeploymentActor to cancel it.
        // This is clearly an invalid state and should not happen.
        case Some(_) =>
          log.error(s"Failed to cancel an invalid deployment ${runningDeployments.get(id)}")

        case None =>
          sender ! DeploymentFailed(
            DeploymentPlan.empty.copy(id = id),
            new DeploymentCanceledException("The upgrade has been cancelled"))
      }

    case msg @ DeploymentFinished(plan) =>
      log.info(s"Removing ${plan.id} from list of running deployments")
      runningDeployments -= plan.id
      deploymentStatus -= plan.id
      deploymentRepository.delete(plan.id)

    case LaunchDeploymentActor(plan, origSender) if isActiveDeployment(plan.id) =>
      val ref = context.actorOf(
        DeploymentActor.props(
          self,
          origSender,
          driver,
          killService,
          scheduler,
          plan,
          taskTracker,
          launchQueue,
          storage,
          healthCheckManager,
          eventBus,
          readinessCheckExecutor
        ),
        plan.id
      )
      runningDeployments += plan.id -> runningDeployments(plan.id).copy(ref = Some(ref), status = Deploying)

    case LaunchDeploymentActor(plan, _) =>
      log.info(s"Deployment $plan was already canceled or overridden by another one. Not proceeding with it")

    case stepInfo: DeploymentStepInfo => deploymentStatus += stepInfo.plan.id -> stepInfo

    case ReadinessCheckUpdate(id, result) => deploymentStatus.get(id).foreach { info =>
      deploymentStatus += id -> info.copy(readinessChecks = info.readinessChecks.updated(result.taskId, result))
    }

    case RetrieveRunningDeployments =>
      sender() ! RunningDeployments(deploymentStatus.values.to[Seq])

    // If the new deployment plan has no conflicts we simply save it in the repository and proceed with the deployment.
    case StartDeployment(plan, origSender, force) if !hasConflicts(plan) =>
      val recipient = sender()
      runningDeployments += plan.id -> DeploymentInfo(None, plan, Scheduled) // 1. Save new plan as [Scheduled]

      async {
        val store = deploymentRepository.store(plan) // 2. Store new plan
        await(store.map(_ => log.info(s"Stored new deployment plan ${plan.id}")))

        if (origSender != Actor.noSender) origSender ! DeploymentStarted(plan) // 2.1 Send response to original sender

        self ! LaunchDeploymentActor(plan, recipient) // 3. Proceed with the deployment
      }

    // If the deployment has conflicts but is not forced, we merely inform the sender about all the deployments
    // that are conflicting with the current one.
    case StartDeployment(plan, origSender, force) if hasConflicts(plan) && !force =>
      origSender ! CommandFailed(
        MarathonSchedulerActor.Deploy(plan, force),
        AppLockedException(conflictingDeployments(plan).map(_.plan.id)))

    // Otherwise we have conflicts and the deployment is forced:
    case StartDeployment(plan, origSender, force) =>
      val recipient = sender()
      // 1. Find all conflicting deployments
      val conflicts = conflictingDeployments(plan)
      val conflictingPlanIds = conflicts.map(_.plan.id)
      log.info(s"Found conflicting deployments $conflictingPlanIds with the actual ${plan.id}")

      // 2. Remove all [Scheduled] deployments (they haven't been started yet, so there is nothing to cancel)
      // and  mark all [Deploying] deployments as [Canceling] so we don't have to cancel them twice
      conflicts.foreach{
        case DeploymentInfo(_, p, Scheduled, _) => runningDeployments.remove(p.id)
        case DeploymentInfo(_, p, Deploying, _) => runningDeployments += p.id -> runningDeployments(p.id).copy(status = Canceling)
      }

      // 3. Save new plan as [Scheduled]
      runningDeployments += plan.id -> DeploymentInfo(None, plan, Scheduled)

      // 4. Delete all conflicts from the repository first and store the new plan afterwards. In this order even
      // if the master crashes we shouldn't have any conflicts stored. However in the worst case (a crash after delete()
      // and before store() we could end up with old deployment canceled and new one not existing.
      //
      // Note: delete() removes conflicting deployment before the actual deployments are canceled. This way the target
      // state of the system is safely saved and even in a case of a crash the new master should be able to reconcile
      // from it.
      async {
        // 4.1 Delete conflicts
        val delete = Future.sequence(conflicts.map(p => deploymentRepository.delete(p.plan.id)))
        await(delete.map(_ => log.info(s"Removed conflicting deployments $conflictingPlanIds from the deployment repository")))

        // 4.2 Store new plan
        val store = deploymentRepository.store(plan)
        await(store.map(_ => log.info(s"Stored new deployment plan ${plan.id}")))

        // 4.3 After the deployment is saved we can send the original sender a positive response
        if (origSender != Actor.noSender) origSender ! DeploymentStarted(plan)

        // 5. Proceed with the deployment
        self ! CancelConflictingDeployments(plan, conflicts, recipient)
      }

    case CancelConflictingDeployments(plan, conflicts, origSender) if isActiveDeployment(plan.id) =>
      val conflictingPlanIds = conflicts.map(_.plan.id)
      // 6. Cancel new conflicting deployments that are not being already canceled (status = [Deploying])
      val newCancellations: Seq[Future[Boolean]] = conflicts.filter(_.status == Deploying).map { info =>
        stopActor(info.ref.get, new DeploymentCanceledException("The upgrade has been cancelled"))
      }

      // 7. Get old conflicting deployments that are already being canceled (status = [Canceling]) and save them
      // together with the new ones so later deployments could hook up to in case this one needs to be canceled too.
      val cancellations: Seq[Future[Boolean]] = conflicts.filter(_.status == Canceling).flatMap(_.cancel) ++ newCancellations
      runningDeployments += plan.id -> runningDeployments(plan.id).copy(cancel = cancellations)

      // When all the conflicting deployments are canceld, proceed with the deployment
      Future.sequence(cancellations).onComplete { _ =>
        log.info(s"Conflicting deployments $conflictingPlanIds for deployment ${plan.id} have been canceled")
        self ! LaunchDeploymentActor(plan, origSender)
      }

    // TODO(AD): do we need to throw a TimeoutException in case canceling conflicting deployments
    // TODO(AD): takes too long? (like MarathonSchedulerActor did)?
  }

  def stopActor(ref: ActorRef, reason: Throwable): Future[Boolean] = {
    val promise = Promise[Boolean]()
    context.actorOf(Props(classOf[StopActor], ref, promise, reason))
    promise.future
  }

  def isActiveDeployment(id: String): Boolean = {
    runningDeployments.contains(id) && runningDeployments(id).status == Scheduled
  }

  def hasConflicts(plan: DeploymentPlan): Boolean = {
    conflictingDeployments(plan).nonEmpty
  }

  /**
    * Methods return all deployments that are currently not being canceled (either Deploying or Scheduled)
    * and are conflicting with passed plan.
    */
  def conflictingDeployments(thisPlan: DeploymentPlan): Seq[DeploymentInfo] = {
    def intersectsWith(thatPlan: DeploymentPlan): Boolean = {
      thatPlan.affectedRunSpecIds.intersect(thisPlan.affectedRunSpecIds).nonEmpty
    }
    runningDeployments.values.filter { info =>
      intersectsWith(info.plan) && info.status != Canceling
    }.to[Seq]
  }
}

object DeploymentManager {
  case class StartDeployment(plan: DeploymentPlan, origSender: ActorRef, force: Boolean = false)
  case class CancelDeployment(id: String)
  case object StopAllDeployments
  case class CancelConflictingDeployments(plan: DeploymentPlan, conflicts: Seq[DeploymentInfo], origSender: ActorRef)
  case class DeploymentLocked(cmd: MarathonSchedulerActor.Deploy)

  case class DeploymentStepInfo(
      plan: DeploymentPlan,
      step: DeploymentStep,
      nr: Int,
      readinessChecks: Map[Task.Id, ReadinessCheckResult] = Map.empty) {
    lazy val readinessChecksByApp: Map[PathId, Seq[ReadinessCheckResult]] = {
      readinessChecks.values.groupBy(_.taskId.runSpecId).mapValues(_.to[Seq]).withDefaultValue(Seq.empty)
    }
  }

  case class DeploymentFinished(plan: DeploymentPlan)
  case class DeploymentFailed(plan: DeploymentPlan, reason: Throwable)
  case class ReadinessCheckUpdate(deploymentId: String, result: ReadinessCheckResult)
  case class LaunchDeploymentActor(plan: DeploymentPlan, origSender: ActorRef)

  case class DeploymentInfo(
    ref: Option[ActorRef], // An ActorRef to the DeploymentActor if status = [Deploying]
    plan: DeploymentPlan, // Deployment plan
    status: DeploymentStatus, // Status can be [Scheduled], [Canceling] or [Deploying]
    cancel: Seq[Future[Boolean]] = Seq.empty) // A sequence of Future[Boolean] that needs to be canceled for this deployment to start

  sealed trait DeploymentStatus
  case object Scheduled extends DeploymentStatus
  case object Canceling extends DeploymentStatus
  case object Deploying extends DeploymentStatus

  def props(
    taskTracker: InstanceTracker,
    killService: KillService,
    launchQueue: LaunchQueue,
    scheduler: SchedulerActions,
    storage: StorageProvider,
    healthCheckManager: HealthCheckManager,
    eventBus: EventStream,
    readinessCheckExecutor: ReadinessCheckExecutor,
    marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
    deploymentRepository: DeploymentRepository): Props = {
    Props(new DeploymentManager(taskTracker, killService, launchQueue,
      scheduler, storage, healthCheckManager, eventBus, readinessCheckExecutor, marathonSchedulerDriverHolder, deploymentRepository))
  }

}
