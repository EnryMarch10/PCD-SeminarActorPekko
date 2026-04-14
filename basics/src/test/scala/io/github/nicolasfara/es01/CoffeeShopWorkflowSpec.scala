package io.github.nicolasfara.es01

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.Millis
import org.scalatest.time.Seconds
import org.scalatest.time.Span
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration.*

class CoffeeShopWorkflowSpec extends ScalaTestWithActorTestKit, AnyWordSpecLike, Eventually, Matchers:

  import actors.CoffeeShopGuardian
  import actors.CoffeeShopGuardian.*
  import CoffeeShopProtocol.*

  "CoffeeShopGuardian" should:
    "eventually mark accepted orders as ready when stock is available" in:
      val guardian = spawn(
        CoffeeShopGuardian(
          initialStock = Map(Espresso -> 2),
          basePreparationTime = 50.millis
        )
      )
      val placementProbe = createTestProbe[OrderPlacementResult]()

      guardian ! PlaceOrder("Alice", List(Espresso), placementProbe.ref)

      val orderId = placementProbe.receiveMessage() match
        case OrderAccepted(id) => id
        case other             => fail(s"Expected OrderAccepted, got $other")

      eventually(timeout(Span(2, Seconds)), interval(Span(50, Millis))):
        val statusProbe = createTestProbe[OrderLookupResult]()
        guardian ! GetOrderStatus(orderId, statusProbe.ref)
        statusProbe.receiveMessage() match
          case OrderStatusSnapshot(`orderId`, "Alice", List(Espresso), OrderStatus.Ready) => ()
          case other => fail(s"Expected Alice's order to be ready, got $other")

    "reject an order when stock is exhausted" in:
      val guardian = spawn(
        CoffeeShopGuardian(
          initialStock = Map(Cappuccino -> 1),
          basePreparationTime = 200.millis
        )
      )
      val firstPlacementProbe = createTestProbe[OrderPlacementResult]()
      val secondPlacementProbe = createTestProbe[OrderPlacementResult]()

      guardian ! PlaceOrder("Bruno", List(Cappuccino), firstPlacementProbe.ref)
      firstPlacementProbe.receiveMessage() match
        case OrderAccepted(_) => ()
        case other            => fail(s"Expected Bruno's order to be accepted, got $other")

      guardian ! PlaceOrder("Carla", List(Cappuccino), secondPlacementProbe.ref)
      secondPlacementProbe.receiveMessage() match
        case OrderRejected("Not enough stock for: Cappuccino") => ()
        case other => fail(s"Expected Carla's order to be rejected, got $other")

    "return the current status before and after preparation" in:
      val guardian = spawn(
        CoffeeShopGuardian(
          initialStock = Map(Tea -> 1),
          basePreparationTime = 400.millis
        )
      )
      val placementProbe = createTestProbe[OrderPlacementResult]()
      val firstStatusProbe = createTestProbe[OrderLookupResult]()

      guardian ! PlaceOrder("Dora", List(Tea), placementProbe.ref)

      val orderId = placementProbe.receiveMessage() match
        case OrderAccepted(id) => id
        case other             => fail(s"Expected OrderAccepted, got $other")

      guardian ! GetOrderStatus(orderId, firstStatusProbe.ref)
      firstStatusProbe.receiveMessage() match
        case OrderStatusSnapshot(`orderId`, "Dora", List(Tea), OrderStatus.InPreparation) => ()
        case other => fail(s"Expected Dora's order to be in preparation, got $other")

      eventually(timeout(Span(2, Seconds)), interval(Span(50, Millis))):
        val finalStatusProbe = createTestProbe[OrderLookupResult]()
        guardian ! GetOrderStatus(orderId, finalStatusProbe.ref)
        finalStatusProbe.receiveMessage() match
          case OrderStatusSnapshot(`orderId`, "Dora", List(Tea), OrderStatus.Ready) => ()
          case other => fail(s"Expected Dora's order to be ready, got $other")
