package io.github.nicolasfara.es01.actors

import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.scaladsl.*
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

object CoffeeShopGuardian:
  import io.github.nicolasfara.es01.CoffeeShopProtocol.*

  enum Command:
    case PlaceOrder(customerName: String, items: List[MenuItem], replyTo: ActorRef[OrderPlacementResult])
    case GetOrderStatus(orderId: UUID, replyTo: ActorRef[OrderLookupResult])

  export Command.*

  def apply(
    initialStock: Map[MenuItem, Int],
    basePreparationTime: FiniteDuration
  ): Behavior[Command] =
    Behaviors.setup: context =>
      val inventory = context.spawn(InventoryActor(initialStock), "inventory")
      val barista = context.spawn(BaristaActor(basePreparationTime), "barista")
      active(context, inventory, barista, Map.empty)

  private def active(
    context: ActorContext[Command],
    inventory: ActorRef[InventoryActor.Command],
    barista: ActorRef[BaristaActor.Command],
    orders: Map[UUID, ActorRef[OrderActor.Command]]
  ): Behavior[Command] =
    Behaviors.receiveMessagePartial:
      case PlaceOrder(_, items, replyTo) if items.isEmpty =>
        replyTo ! OrderRejected("An order must contain at least one item")
        Behaviors.same
      case PlaceOrder(customerName, items, replyTo) =>
        val orderId = UUID.randomUUID()
        val actorName = s"order-${orderId.toString.replace("-", "")}"
        val orderActor = context.spawn(
          OrderActor(orderId, customerName, items, inventory, barista, replyTo),
          actorName
        )
        context.log.info("Created order {} for {} with items {}", orderId, customerName, items)
        active(context, inventory, barista, orders.updated(orderId, orderActor))
      case GetOrderStatus(orderId, replyTo) =>
        orders.get(orderId) match
          case Some(orderActor) => orderActor ! OrderActor.GetStatus(replyTo)
          case None             => replyTo ! OrderNotFound(orderId)
        Behaviors.same

