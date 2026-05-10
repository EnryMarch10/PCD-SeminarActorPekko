package io.github.nicolasfara.es01.cluster.sharding

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import io.github.nicolasfara.es01.cluster.CborSerializable
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey

object Counter:
  // EntityTypeKey defines the name of the shard region and the type of messages it accepts
  val TypeKey: EntityTypeKey[Command] = EntityTypeKey[Command]("Counter")

  // The commands that the Counter entity can process
  sealed trait Command extends CborSerializable
  case object Increment extends Command
  final case class GetValue(replyTo: ActorRef[Summary]) extends Command

  // The reply message for GetValue
  final case class Summary(entityId: String, value: Int, hostedOnNode: String) extends CborSerializable

  // The behavior factory function, receives the entityId
  def apply(entityId: String): Behavior[Command] = Behaviors.setup { context =>
    context.log.info("Starting Counter entity {} on node {}", entityId, context.system.address)
    counter(entityId, 0)
  }

  private def counter(entityId: String, value: Int): Behavior[Command] = Behaviors.receive { (context, message) =>
    message match
      case Increment =>
        context.log.debug("Incrementing counter {} to {}", entityId, value + 1)
        counter(entityId, value + 1)
      
      case GetValue(replyTo) =>
        val hostAddress = context.system.address.toString
        replyTo ! Summary(entityId, value, hostAddress)
        Behaviors.same
  }
