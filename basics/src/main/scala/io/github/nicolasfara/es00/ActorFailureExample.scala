package io.github.nicolasfara.es00

import org.apache.pekko.actor.typed.scaladsl.*
import org.apache.pekko.actor.typed.*

class SupervisorActor(context: ActorContext[String]) extends AbstractBehavior[String](context):
  val child = context.spawn(
    Behaviors.supervise(SupervisedActor())
      .onFailure[Exception](SupervisorStrategy.restart),
    "child-actor"
  )

  def onMessage(msg: String): Behavior[String] = msg match
    case "fail-child" =>
      child ! "fail"
      this

object SupervisorActor:
  def apply(): Behavior[String] = Behaviors.setup(ctx => new SupervisorActor(ctx))

class SupervisedActor(context: ActorContext[String]) extends AbstractBehavior[String](context):
  def onMessage(msg: String): Behavior[String] = msg match
    case "fail" =>
      println(s"Child ${context.self} failing now")
      throw new Exception("I failed!")

object SupervisedActor:
  def apply(): Behavior[String] = Behaviors.setup(ctx => new SupervisedActor(ctx))

@main def runFailure(): Unit =
  val system = ActorSystem(SupervisorActor(), "HelloAkka")
  system ! "fail-child"
