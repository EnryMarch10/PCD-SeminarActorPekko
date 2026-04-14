package io.github.nicolasfara.es01.actors

import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.scaladsl.*
import java.util.UUID

object OrderActor:
  import io.github.nicolasfara.es01.CoffeeShopProtocol.*

  enum Command:
    case GetStatus(replyTo: ActorRef[OrderLookupResult])
    case WrappedReservation(result: InventoryActor.ReservationResult)
    case WrappedPreparation(result: BaristaActor.CoffeeReady)

  export Command.*

  def apply(
    orderId: UUID,
    customerName: String,
    items: List[MenuItem],
    inventory: ActorRef[InventoryActor.Command],
    barista: ActorRef[BaristaActor.Command],
    placementReplyTo: ActorRef[OrderPlacementResult]
  ): Behavior[Command] =
    Behaviors.setup: context =>
      val reservationAdapter =
        context.messageAdapter[InventoryActor.ReservationResult](WrappedReservation.apply)
      val preparationAdapter =
        context.messageAdapter[BaristaActor.CoffeeReady](WrappedPreparation.apply)

      inventory ! InventoryActor.Reserve(orderId, items, reservationAdapter)
      pendingInventory(context, orderId, customerName, items, barista, preparationAdapter, placementReplyTo)

  private def snapshot(orderId: UUID, customerName: String, items: List[MenuItem], status: OrderStatus): OrderStatusSnapshot =
    OrderStatusSnapshot(orderId, customerName, items, status)

  private def pendingInventory(
    context: ActorContext[Command],
    orderId: UUID,
    customerName: String,
    items: List[MenuItem],
    barista: ActorRef[BaristaActor.Command],
    preparationAdapter: ActorRef[BaristaActor.CoffeeReady],
    placementReplyTo: ActorRef[OrderPlacementResult]
  ): Behavior[Command] =
    Behaviors.receiveMessagePartial:
      case GetStatus(replyTo) =>
        replyTo ! snapshot(orderId, customerName, items, OrderStatus.PendingInventory)
        Behaviors.same
      case WrappedReservation(InventoryActor.StockReserved(`orderId`)) =>
        placementReplyTo ! OrderAccepted(orderId)
        barista ! BaristaActor.Prepare(orderId, items, preparationAdapter)
        inPreparation(context, orderId, customerName, items)
      case WrappedReservation(InventoryActor.StockRejected(`orderId`, reason)) =>
        placementReplyTo ! OrderRejected(reason)
        rejected(orderId, customerName, items)

  private def inPreparation(context: ActorContext[Command], orderId: UUID, customerName: String, items: List[MenuItem]): Behavior[Command] =
    Behaviors.receiveMessagePartial:
      case GetStatus(replyTo) =>
        replyTo ! snapshot(orderId, customerName, items, OrderStatus.InPreparation)
        Behaviors.same
      case WrappedPreparation(BaristaActor.CoffeeReady(`orderId`)) =>
        context.log.info("Order {} moved to ready", orderId)
        ready(orderId, customerName, items)

  private def ready(orderId: UUID, customerName: String, items: List[MenuItem]): Behavior[Command] =
    Behaviors.receiveMessagePartial:
      case GetStatus(replyTo) =>
        replyTo ! snapshot(orderId, customerName, items, OrderStatus.Ready)
        Behaviors.same

  private def rejected(orderId: UUID, customerName: String, items: List[MenuItem]): Behavior[Command] =
    Behaviors.receiveMessagePartial:
      case GetStatus(replyTo) =>
        replyTo ! snapshot(orderId, customerName, items, OrderStatus.Rejected)
        Behaviors.same
