package io.github.nicolasfara.es01.cluster.factorial

import org.apache.pekko.actor.typed.*
import io.github.nicolasfara.es01.cluster.CborSerializable
import org.apache.pekko.actor.typed.scaladsl.*
import org.apache.pekko.actor.typed.receptionist.*

object Worker:
  enum Command extends CborSerializable:
    case EvalFactorial(n: Int, replyTo: ActorRef[Command.Result])
    case Result(n: Int, result: BigInt)

  export Command.*

  def apply(): Behavior[Command] = Behaviors.setup: _ =>
    Behaviors.receiveMessagePartial:
      case EvalFactorial(n, replyTo) =>
        val result = factorial(n)
        replyTo ! Result(n, result)
        Behaviors.same

  private def factorial(n: Int): BigInt =
    @annotation.tailrec
    def loop(acc: BigInt, n: Int): BigInt =
      if n <= 1 then acc
      else loop(acc * n, n - 1)
    loop(BigInt(1), n)

object WorkerRouter:
  val workerServiceKey = ServiceKey[Worker.Command]("worker")
  def apply(workers: Int): Behavior[Unit] = Behaviors.setup: ctx =>
    (0 to workers - 1).map(i => ctx.spawn(Worker(), s"worker-$i")).foreach: ref =>
      ctx.system.receptionist ! Receptionist.Register(workerServiceKey, ref)
    Behaviors.empty