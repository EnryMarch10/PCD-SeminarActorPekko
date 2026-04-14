package io.github.nicolasfara.es00

import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.scaladsl.*

object RequestResponseProtocol:

  enum RequesterCommand:
    case Start
    case GotResponse(response: String)

  enum ResponderCommand:
    case Request(msg: String, replyTo: ActorRef[RequesterCommand])

object ResponderActor:

  export RequestResponseProtocol.*
  export RequestResponseProtocol.ResponderCommand.*

  def apply(): Behavior[ResponderCommand] = Behaviors.receive: (context, message) =>
    message match
      case Request(msg, replyTo) =>
        context.log.info(s"Responder received: $msg")
        replyTo ! RequestResponseProtocol.RequesterCommand.GotResponse(s"ack: $msg")
        Behaviors.same

object RequesterActor:

  export RequestResponseProtocol.*
  export RequestResponseProtocol.RequesterCommand.*

  def apply(responder: ActorRef[ResponderActor.ResponderCommand]): Behavior[RequesterCommand] =
    Behaviors.receive: (context, message) =>
      message match
        case Start =>
          context.log.info("Requester sending request")
          responder ! ResponderActor.Request("hello from requester", context.self)
          Behaviors.same
        case GotResponse(response) =>
          context.log.info(s"Requester received response: $response")
          Behaviors.stopped

object RequestResponseGuardian:

  def apply(): Behavior[RequestResponseProtocol.RequesterCommand] = Behaviors.setup: context =>
    val responder = context.spawn(ResponderActor(), "responder")
    RequesterActor(responder)

@main def runRequesterResponser(): Unit =
  val system = ActorSystem(RequestResponseGuardian(), "HelloAkka")
  system ! RequestResponseProtocol.RequesterCommand.Start
