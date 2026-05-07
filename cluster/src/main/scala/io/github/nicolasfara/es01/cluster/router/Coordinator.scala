package io.github.nicolasfara.es01.cluster.router

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.receptionist.*
import org.apache.pekko.actor.typed.scaladsl.*

import scala.concurrent.duration.*

object Coordinator:
  sealed trait Command
  private case object Tick extends Command
  private final case class WrappedResult(result: Worker.Result) extends Command
  private final case class WorkersUpdated(workers: Set[ActorRef[Worker.Command]]) extends Command

  private val inputs = Vector(5, 10, 15, 20)
  private val requestInterval = 2.seconds

  def apply(): Behavior[Command] =
    Behaviors.setup: ctx =>
      Behaviors.withTimers: timers =>
        val router = ctx.spawn(Routers.group(WorkerRouter.workerServiceKey), "worker-router")
        val resultAdapter = ctx.messageAdapter[Worker.Result](WrappedResult.apply)
        val listingAdapter = ctx.messageAdapter[Receptionist.Listing]:
          case WorkerRouter.workerServiceKey.Listing(workers) => WorkersUpdated(workers)

        ctx.system.receptionist ! Receptionist.Subscribe(WorkerRouter.workerServiceKey, listingAdapter)

        def idle(nextInputIdx: Int): Behavior[Command] =
          Behaviors.receiveMessage:
            case WorkersUpdated(workers) if workers.nonEmpty =>
              ctx.log.info("Discovered {} workers, starting periodic factorial requests", workers.size)
              timers.startTimerAtFixedRate(Tick, Tick, requestInterval)
              active(workers.size, nextInputIdx)
            case WorkersUpdated(_) =>
              Behaviors.same
            case WrappedResult(Worker.Result(n, result)) =>
              ctx.log.info("Factorial of {} is {}", n, result)
              Behaviors.same
            case Tick =>
              Behaviors.same

        def active(workerCount: Int, nextInputIdx: Int): Behavior[Command] =
          Behaviors.receiveMessage:
            case WorkersUpdated(workers) if workers.isEmpty =>
              ctx.log.info("No workers available, stopping periodic factorial requests")
              timers.cancel(Tick)
              idle(nextInputIdx)
            case WorkersUpdated(workers) =>
              if workers.size != workerCount then
                ctx.log.info("Workers available: {}", workers.size)
              active(workers.size, nextInputIdx)
            case Tick =>
              val updatedInputIdx = (0 until workerCount).foldLeft(nextInputIdx): (idx, _) =>
                val n = inputs(idx % inputs.size)
                router ! Worker.EvalFactorial(n, resultAdapter)
                idx + 1
              active(workerCount, updatedInputIdx)
            case WrappedResult(Worker.Result(n, result)) =>
              ctx.log.info("Factorial of {} is {}", n, result)
              Behaviors.same

        idle(0)

@main def spawnCoordinator(): Unit =
  val config = ConfigFactory.load("application-router.conf")
  val _ = ActorSystem[Coordinator.Command](Coordinator(), "ClusterSystem", config)

@main def spawnWorker(): Unit =
  val config = ConfigFactory.load("application-router.conf")
  val _ = ActorSystem[Unit](WorkerRouter(10), "ClusterSystem", config)
