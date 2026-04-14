package io.github.nicolasfara.es01.actors

import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.scaladsl.*
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

object BaristaActor:
  import io.github.nicolasfara.es01.CoffeeShopProtocol.*

  enum Command:
    case Prepare(orderId: UUID, items: List[MenuItem], replyTo: ActorRef[CoffeeReady])
    case Complete(orderId: UUID)

  export Command.*

  final case class CoffeeReady(orderId: UUID)

  def apply(basePreparationTime: FiniteDuration): Behavior[Command] =
    Behaviors.withTimers: timers =>
      active(timers, basePreparationTime, Map.empty)

  private def active(
    timers: TimerScheduler[Command],
    basePreparationTime: FiniteDuration,
    pendingOrders: Map[UUID, ActorRef[CoffeeReady]]
  ): Behavior[Command] =
    Behaviors.receive: (context, message) =>
      message match
        case Prepare(orderId, items, replyTo) =>
          val delay = basePreparationTime * math.max(1, items.size.toLong)
          context.log.info("Starting preparation for order {}. It will take {}", orderId, delay)
          timers.startSingleTimer(orderId, Complete(orderId), delay)
          active(timers, basePreparationTime, pendingOrders.updated(orderId, replyTo))
        case Complete(orderId) =>
          pendingOrders.get(orderId).foreach: customer =>
            context.log.info("Order {} is ready for pickup", orderId)
            customer ! CoffeeReady(orderId)
          active(timers, basePreparationTime, pendingOrders - orderId)
