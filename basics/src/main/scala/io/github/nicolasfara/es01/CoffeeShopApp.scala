package io.github.nicolasfara.es01

import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.scaladsl.*
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.util.Timeout
import io.github.nicolasfara.es01.actors.CoffeeShopGuardian
import io.github.nicolasfara.es01.CoffeeShopProtocol.*

import scala.concurrent.duration.*
import scala.concurrent.*

import java.util.UUID

object CoffeeShopApp:
  /** Retrieves the status of an order by its id, returning a Future that will complete with the result. */
  def getOrderStatus(orderId: UUID)(using system: ActorSystem[CoffeeShopGuardian.Command], timeout: Timeout): Future[OrderLookupResult] =
    system ? (CoffeeShopGuardian.GetOrderStatus(orderId, _))

  /** Places an order for a customer with the specified items, returning a Future that will complete with the result of the order placement. */
  def placeOrder(customerName: String, items: List[MenuItem])(using system: ActorSystem[CoffeeShopGuardian.Command], timeout: Timeout): Future[OrderPlacementResult] =
    system ? (CoffeeShopGuardian.PlaceOrder(customerName, items, _))

  @main def app(): Unit =
    import actors.CoffeeShopGuardian
    import CoffeeShopProtocol.*

    given timeout: Timeout = 5.seconds

    val initialStock = Map(
      Espresso -> 2,
      Cappuccino -> 1,
      Tea -> 2,
      Croissant -> 1
    )

    val system = ActorSystem(
      CoffeeShopGuardian(initialStock, basePreparationTime = 600.millis),
      "CoffeeShopDemo"
    )
    given ActorSystem[CoffeeShopGuardian.Command] = system
    given ExecutionContext = system.executionContext

    system.log.info("Coffee shop is open with initial stock: {}", initialStock)
    system.log.info("Retriving status for a non-existent order...")
    val nonExistentId = UUID.randomUUID()
    val nonExistingOrderStatus = getOrderStatus(nonExistentId)
    val result = Await.result(nonExistingOrderStatus, timeout.duration)
    system.log.warn("Status for non-existent order {}: {}", nonExistentId, result)

    val aliceOrder = for
      placementResult <- placeOrder("Alice", List(Espresso, Croissant))
      orderId <- placementResult match
        case OrderAccepted(id) => Future.successful(id)
        case OrderRejected(reason) => Future.failed[UUID](new RuntimeException(s"Alice's order was rejected: $reason"))
      _ = system.log.info("Alice's order was accepted with id {}", orderId)
      _ = system.log.info("Checking status for Alice's order immediately after placement...")
      initialStatus <- getOrderStatus(orderId)
      _ = system.log.info("Initial status for Alice's order {}: {}", orderId, initialStatus)
      _ = system.log.info("Waiting for Alice's order to be ready...")
      finalStatus <- getOrderStatus(orderId).map:
        case OrderStatusSnapshot(`orderId`, "Alice", _, OrderStatus.Ready) =>
          system.log.info("Alice's order {} is ready!", orderId)
          OrderStatus.Ready
        case other =>
          system.log.info("Alice's order {} is not ready yet, current status: {}", orderId, other)
          OrderStatus.InPreparation
    yield finalStatus
    val aliceResult = Await.result(aliceOrder, timeout.duration)
    system.log.info("Final status for Alice's order: {}", aliceResult)

    // def placeOrderAsync(customerName: String, items: List[MenuItem]): Future[OrderPlacementResult] =
    //   system ? (replyTo => PlaceOrder(customerName, items, replyTo))

    // def placeOrder(customerName: String, items: List[MenuItem]): OrderPlacementResult =
    //   Await.result(placeOrderAsync(customerName, items), 5.seconds)

    // def getStatusAsync(orderId: UUID): Future[OrderLookupResult] =
    //   system ? (replyTo => GetOrderStatus(orderId, replyTo))

    // def getStatus(orderId: UUID): OrderLookupResult =
    //   Await.result(getStatusAsync(orderId), 5.seconds)

    // def waitUntilReady(orderId: UUID): Unit =
    //   def waitForReady(): Future[Unit] =
    //     getStatusAsync(orderId).flatMap:
    //       case OrderStatusSnapshot(_, _, _, OrderStatus.Ready) => Future.unit
    //       case _ =>
    //         val promise = Promise[Unit]()
    //         val retry = new Runnable:
    //           override def run(): Unit = promise.completeWith(waitForReady())
    //         val _ = system.scheduler.scheduleOnce(200.millis, retry)
    //         promise.future

    //   Await.result(waitForReady(), 5.seconds)

    // def renderPlacement(result: OrderPlacementResult): String =
    //   result match
    //     case OrderAccepted(orderId) => s"accepted with id $orderId"
    //     case OrderRejected(reason)  => s"rejected: $reason"

    // def renderStatus(result: OrderLookupResult): String =
    //   result match
    //     case OrderStatusSnapshot(orderId, customerName, items, status) =>
    //       s"order $orderId for $customerName -> ${items.mkString("[", ", ", "]")} is $status"
    //     case OrderNotFound(orderId) =>
    //       s"order $orderId was not found"

    // println("Placing three orders...")

    // val alicePlacement = placeOrder("Alice", List(Espresso))
    // println(s"Alice's order was ${renderPlacement(alicePlacement)}")

    // val brunoPlacement = placeOrder("Bruno", List(Cappuccino, Croissant))
    // println(s"Bruno's order was ${renderPlacement(brunoPlacement)}")

    // val carlaPlacement = placeOrder("Carla", List(Cappuccino))
    // println(s"Carla's order was ${renderPlacement(carlaPlacement)}")

    // val acceptedOrders = List(alicePlacement, brunoPlacement).collect:
    //   case OrderAccepted(orderId) => orderId

    // println()
    // println("Immediate status check:")
    // acceptedOrders.foreach: orderId =>
    //   println(renderStatus(getStatus(orderId)))

    // acceptedOrders.foreach(waitUntilReady)

    // println()
    // println("Status check after preparation:")
    // acceptedOrders.foreach: orderId =>
    //   println(renderStatus(getStatus(orderId)))

    system.terminate()

