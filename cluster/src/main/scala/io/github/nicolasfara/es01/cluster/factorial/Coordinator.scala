package io.github.nicolasfara.es01.cluster.factorial

import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.scaladsl.*
import io.github.nicolasfara.es01.cluster.CborSerializable
import org.apache.pekko.actor.typed.receptionist.*

object Coordinator:
  case object Tick extends CborSerializable

  def apply(): Behavior[Worker.Result] = Behaviors.withTimers: _ =>
    Behaviors.setup: ctx =>
      val router = Routers.group(WorkerRouter.workerServiceKey)
      val ref = ctx.spawn(router, "worker-router")
      // Wait for workers to register
      val _ = ctx.spawnAnonymous[Receptionist.Listing](
        Behaviors.setup: ctx2 =>
          ctx.system.receptionist ! Receptionist.Subscribe(WorkerRouter.workerServiceKey, ctx2.self)
          Behaviors.receiveMessagePartial:
            case WorkerRouter.workerServiceKey.Listing(workers) if workers.nonEmpty =>
              val inputs = List(5, 10, 15, 20)
              inputs.foreach(n => ref ! Worker.EvalFactorial(n, ctx.self))
              Behaviors.same
      )
      Behaviors.receiveMessage:
        case Worker.Result(n, result) =>
          ctx.log.info(s"Factorial of $n is $result")
          Behaviors.same

@main def 