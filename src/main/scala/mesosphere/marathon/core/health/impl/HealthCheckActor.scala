package mesosphere.marathon.core.health.impl

import akka.actor.{ Actor, ActorLogging, ActorRef, Cancellable, Props }
import akka.event.EventStream
import mesosphere.marathon.core.event._
import mesosphere.marathon.core.health.impl.HealthCheckActor._
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.core.health._
import mesosphere.marathon.core.instance.LegacyAppInstance
import mesosphere.marathon.state.{ AppDefinition, Timestamp }
import mesosphere.marathon.core.task.termination.{ KillReason, KillService }

import scala.concurrent.duration._

private[health] class HealthCheckActor(
    app: AppDefinition,
    killService: KillService,
    healthCheck: HealthCheck,
    taskTracker: InstanceTracker,
    eventBus: EventStream) extends Actor with ActorLogging {

  import context.dispatcher
  import HealthCheckWorker.HealthCheckJob

  var nextScheduledCheck: Option[Cancellable] = None
  var instanceHealth = Map[Task.Id, Health]()

  val workerProps = Props[HealthCheckWorkerActor]

  override def preStart(): Unit = {
    log.info(
      "Starting health check actor for app [{}] version [{}] and healthCheck [{}]",
      app.id,
      app.version,
      healthCheck
    )
    //Start health checking not after the default first health check
    val start = math.min(healthCheck.interval.toMillis, HealthCheck.DefaultFirstHealthCheckAfter.toMillis).millis
    scheduleNextHealthCheck(Some(start))
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit =
    log.info(
      "Restarting health check actor for app [{}] version [{}] and healthCheck [{}]",
      app.id,
      app.version,
      healthCheck
    )

  override def postStop(): Unit = {
    nextScheduledCheck.forall { _.cancel() }
    log.info(
      "Stopped health check actor for app [{}] version [{}] and healthCheck [{}]",
      app.id,
      app.version,
      healthCheck
    )
  }

  def purgeStatusOfDoneTasks(): Unit = {
    log.debug(
      "Purging health status of done tasks for app [{}] version [{}] and healthCheck [{}]",
      app.id,
      app.version,
      healthCheck
    )
    val activeTaskIds: Set[Task.Id] = taskTracker.specInstancesLaunchedSync(app.id).flatMap(_.tasksMap.keys)(collection.breakOut)
    // The Map built with filterKeys wraps the original map and contains a reference to activeTaskIds.
    // Therefore we materialize it into a new map.
    instanceHealth = instanceHealth.filterKeys(activeTaskIds).iterator.toMap
  }

  def scheduleNextHealthCheck(interval: Option[FiniteDuration] = None): Unit = healthCheck match {
    case hc: MarathonHealthCheck =>
      log.debug(
        "Scheduling next health check for app [{}] version [{}] and healthCheck [{}]",
        app.id,
        app.version,
        hc
      )
      nextScheduledCheck = Some(
        context.system.scheduler.scheduleOnce(interval.getOrElse(hc.interval)) {
          self ! Tick
        }
      )
    case _ => // Don't do anything for Mesos health checks
  }

  def dispatchJobs(): Unit = healthCheck match {
    case hc: MarathonHealthCheck =>
      log.debug("Dispatching health check jobs to workers")
      taskTracker.specInstancesSync(app.id).foreach { instance =>
        instance.tasksMap.foreach {
          case (taskId, task) =>
            if (task.runSpecVersion == app.version && task.isRunning) {
              log.debug("Dispatching health check job for {}", taskId)
              val worker: ActorRef = context.actorOf(workerProps)
              worker ! HealthCheckJob(app, task, hc)
            }
        }
      }
    case _ => // Don't do anything for Mesos health checks
  }

  def checkConsecutiveFailures(task: Task, health: Health): Unit = {
    val consecutiveFailures = health.consecutiveFailures
    val maxFailures = healthCheck.maxConsecutiveFailures

    // ignore failures if maxFailures == 0
    if (consecutiveFailures >= maxFailures && maxFailures > 0) {
      log.info(
        s"Detected unhealthy ${task.taskId} of app [${app.id}] version [${app.version}] on host ${task.agentInfo.host}"
      )

      // kill the task, if it is reachable
      if (task.isUnreachable) {
        val id = task.taskId
        log.info(s"Task $id on host ${task.agentInfo.host} is temporarily unreachable. Performing no kill.")
      } else {
        log.info(s"Send kill request for ${task.taskId} on host ${task.agentInfo.host} to driver")
        eventBus.publish(
          UnhealthyTaskKillEvent(
            appId = task.runSpecId,
            taskId = task.taskId,
            version = app.version,
            reason = health.lastFailureCause.getOrElse("unknown"),
            host = task.agentInfo.host,
            slaveId = task.agentInfo.agentId,
            timestamp = health.lastFailure.getOrElse(Timestamp.now()).toString
          )
        )
        killService.killInstance(LegacyAppInstance(task), KillReason.FailedHealthChecks)
      }
    }
  }

  def ignoreFailures(task: Task, health: Health): Boolean = {
    // Ignore failures during the grace period, until the task becomes healthy
    // for the first time. Also ignore failures while the task is created, starting or staging.
    // TODO: wouldn't it be simpler and still correct to ignore all tasks that are not Running? (DCOS-10332)
    task.launched.fold(true) { launched =>
      health.firstSuccess.isEmpty &&
        task.status.startedAt.fold(true) { startedAt =>
          startedAt + healthCheck.gracePeriod > Timestamp.now()
        }
    }
  }

  def receive: Receive = {
    case GetTaskHealth(taskId) => sender() ! instanceHealth.getOrElse(taskId, Health(taskId))

    case GetAppHealth =>
      sender() ! AppHealth(instanceHealth.values.toSeq)

    case Tick =>
      purgeStatusOfDoneTasks()
      dispatchJobs()
      scheduleNextHealthCheck()

    case result: HealthResult if result.version == app.version =>
      log.info("Received health result for app [{}] version [{}]: [{}]", app.id, app.version, result)
      val taskId = result.taskId
      val health = instanceHealth.getOrElse(taskId, Health(taskId))

      val newHealth = result match {
        case Healthy(_, _, _, _) =>
          health.update(result)
        case Unhealthy(_, _, _, _, _) =>
          // TODO(PODS): load the instance associated with this task and see if it contains that task
          taskTracker.instancesBySpecSync.task(taskId) match {
            case Some(task) =>
              if (ignoreFailures(task, health)) {
                // Don't update health
                health
              } else {
                log.debug("Task {} is {}", task.taskId, result)
                if (result.publishEvent) {
                  eventBus.publish(FailedHealthCheck(app.id, taskId, healthCheck))
                }
                checkConsecutiveFailures(task, health)
                health.update(result)
              }
            case None =>
              log.error(s"Couldn't find task $taskId")
              health.update(result)
          }
      }

      instanceHealth += (taskId -> newHealth)

      if (health.alive != newHealth.alive && result.publishEvent) {
        eventBus.publish(HealthStatusChanged(app.id, taskId, result.version, alive = newHealth.alive))
        // We moved to InstanceHealthChanged Events everywhere
        // Since we perform marathon based health checks only for apps, (every task is an instance)
        // every health result is translated to an instance health changed event
        eventBus.publish(InstanceHealthChanged(taskId.instanceId, result.version, app.id, Some(newHealth.alive)))
      }

    case result: HealthResult =>
      log.warning(s"Ignoring health result [$result] due to version mismatch.")

  }
}

object HealthCheckActor {
  def props(
    app: AppDefinition,
    killService: KillService,
    healthCheck: HealthCheck,
    taskTracker: InstanceTracker,
    eventBus: EventStream): Props = {

    Props(new HealthCheckActor(
      app,
      killService,
      healthCheck,
      taskTracker,
      eventBus))
  }

  // self-sent every healthCheck.intervalSeconds
  case object Tick
  case class GetTaskHealth(instanceId: Task.Id)
  case object GetAppHealth

  case class AppHealth(health: Seq[Health])
}
