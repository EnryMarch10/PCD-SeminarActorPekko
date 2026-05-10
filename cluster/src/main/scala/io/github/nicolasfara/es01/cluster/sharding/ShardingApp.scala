package io.github.nicolasfara.es01.cluster.sharding

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorSystem, Behavior}
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration.*
import scala.util.Random

object ShardingApp:
  
  sealed trait Command
  private case object Tick extends Command
  private case class WrappedCounterResponse(response: Counter.Summary) extends Command

  def rootBehavior(): Behavior[Command] = Behaviors.setup { context =>
    // Initialize Sharding
    val sharding = ClusterSharding(context.system)

    // Register the Counter entity with sharding
    val _ = sharding.init(Entity(typeKey = Counter.TypeKey) { entityContext =>
      Counter(entityContext.entityId)
    })

    // Create a message adapter to receive responses from Counters
    val responseAdapter = context.messageAdapter[Counter.Summary](WrappedCounterResponse.apply)

    // Start a timer to periodically send commands
    Behaviors.withTimers { timers =>
      timers.startTimerWithFixedDelay(Tick, 2.seconds)

      Behaviors.receiveMessage:
        case Tick =>
          val counterId = s"counter-${Random.nextInt(3) + 1}" // Randomly pick counter-1, counter-2, or counter-3
          context.log.info(s"TICK: Sending Increment and GetValue to entity [${counterId}]")
          // Obtain an ActorRef to the specific shard entity
          val counterRef = sharding.entityRefFor(Counter.TypeKey, counterId)
          // Send Increment
          counterRef ! Counter.Increment
          // Ask for the current value
          // Note: using tell (!) with an adapter since we are in an actor behavior
          counterRef ! Counter.GetValue(responseAdapter)
          
          Behaviors.same

        case WrappedCounterResponse(summary) =>
          context.log.info(
            "| SHARDING DEMO | Entity: {} | Value: {} | Hosted On: {} | Sent From: {}",
            summary.entityId,
            summary.value,
            summary.hostedOnNode,
            context.system.address
          )
          Behaviors.same
    }
  }

  def main(args: Array[String]): Unit =
    val ports =
      if args.nonEmpty then args.toSeq.map(_.toInt)
      else sys.env.get("CLUSTER_PORT").flatMap(_.toIntOption).map(Seq(_)).getOrElse(Seq(25251, 25252, 0))

    ports.foreach(startup)

  private def startup(port: Int): Unit =
    val config = ConfigFactory.parseString(s"""
      pekko.remote.artery.canonical.port = $port
      """).withFallback(ConfigFactory.load("application-sharding.conf"))

    val _ = ActorSystem(rootBehavior(), "ClusterSystem", config)
