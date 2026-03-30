package io.github.nicolasfara.es00

import org.apache.pekko.actor.typed.scaladsl.*
import org.apache.pekko.actor.typed.*

class StartStopActor(context: ActorContext[String]) extends AbstractBehavior[String](context):
  println(s"Actor ${context.self} started")
  val _ = context.spawn(ChildActor(), "child-actor")

  def onMessage(msg: String): Behavior[String] = msg match
    case "stop" => Behaviors.stopped
  
  override def onSignal: PartialFunction[Signal, Behavior[String]] =
    case PostStop =>
      println(s"Actor ${context.self} stopping")
      this

object StartStopActor:
  def apply(): Behavior[String] = Behaviors.setup(ctx => new StartStopActor(ctx))

class ChildActor(context: ActorContext[String]) extends AbstractBehavior[String](context):
  println(s"Actor ${context.self} started")

  def onMessage(msg: String): Behavior[String] = Behaviors.unhandled
  override def onSignal: PartialFunction[Signal, Behavior[String]] =
    case PostStop =>
      println(s"Actor ${context.self} stopping")
      this

object ChildActor:
  def apply(): Behavior[String] = Behaviors.setup(ctx => new ChildActor(ctx))

@main def runLifecycle(): Unit =
  val system = ActorSystem(StartStopActor(), "HelloAkka")
  system ! "stop"
