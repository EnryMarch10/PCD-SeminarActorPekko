package io.github.nicolasfara.es00

import org.apache.pekko.actor.typed.scaladsl.*
import org.apache.pekko.actor.typed.*

object ParentActor:
  def apply(): Behavior[String] = Behaviors.setup: ctx =>
    ctx.watch(ctx.spawn(ChildFailingActor(), "child-actor"))
    Behaviors
      .receiveSignal:
        case (_, Terminated(ref)) =>
          println(s"Child $ref has terminated")
          Behaviors.stopped

object ChildFailingActor:
  def apply(): Behavior[String] = Behaviors.setup: _ =>
    throw new RuntimeException("I failed")

@main def runParentFailureNotification(): Unit =
  val _ = ActorSystem(ParentActor(), "HelloAkka")
