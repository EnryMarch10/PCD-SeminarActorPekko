package io.github.nicolasfara.es01

import java.util.UUID

object CoffeeShopProtocol:
  enum MenuItem:
    case Espresso
    case Cappuccino
    case Tea
    case Croissant

  enum OrderStatus:
    case PendingInventory
    case InPreparation
    case Ready
    case Rejected

  enum OrderPlacementResult:
    case OrderAccepted(orderId: UUID)
    case OrderRejected(reason: String)

  enum OrderLookupResult:
    case OrderStatusSnapshot(orderId: UUID, customerName: String, items: List[MenuItem], status: OrderStatus)
    case OrderNotFound(orderId: UUID)

  export MenuItem.*
  export OrderStatus.*
  export OrderPlacementResult.*
  export OrderLookupResult.*
