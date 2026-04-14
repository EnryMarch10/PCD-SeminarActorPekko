package io.github.nicolasfara.es01.actors

import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.scaladsl.*
import java.util.UUID

object InventoryActor:
  import io.github.nicolasfara.es01.CoffeeShopProtocol.*

  enum Command:
    case Reserve(orderId: UUID, items: List[MenuItem], replyTo: ActorRef[ReservationResult])

  export Command.*

  enum ReservationResult:
    case StockReserved(orderId: UUID)
    case StockRejected(orderId: UUID, reason: String)

  export ReservationResult.*

  def apply(initialStock: Map[MenuItem, Int]): Behavior[Command] =
    active(initialStock.withDefaultValue(0))

  private def active(stock: Map[MenuItem, Int]): Behavior[Command] =
    Behaviors.receive: (context, message) =>
      message match
        case Reserve(orderId, items, replyTo) =>
          val requestedItems = items.groupMapReduce(identity)(_ => 1)(_ + _)
          val missingItems = requestedItems.toList
            .sortBy((item, _) => item.ordinal)
            .collect:
              case (item, amountNeeded) if stock(item) < amountNeeded => item
          if missingItems.nonEmpty then
            val reason = missingItems.mkString("Not enough stock for: ", ", ", "")
            context.log.info("Rejecting order {} because {}", orderId, reason)
            replyTo ! StockRejected(orderId, reason)
            Behaviors.same
          else
            val updatedStock = requestedItems.foldLeft(stock):
              case (currentStock, (item, amountNeeded)) =>
                currentStock.updated(item, currentStock(item) - amountNeeded)
            context.log.info("Reserved stock for order {}. Remaining stock: {}", orderId, updatedStock)
            replyTo ! StockReserved(orderId)
            active(updatedStock)
