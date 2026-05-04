package io.github.nicolasfara.es01.cluster.simple

import org.apache.pekko.actor.typed.scaladsl.*
import org.apache.pekko.actor.typed.Behavior
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.typed.ActorSystem

object App:
  object RootBehavior:
    def apply(): Behavior[Nothing] = Behaviors.setup[Nothing] { context =>
      val _ = context.spawn(ClusterListener(), "cluster-listener")
      Behaviors.empty
    }

  def main(args: Array[String]): Unit =
    val port = if args.isEmpty then Seq(25251, 25252, 0) else args.toSeq.map(_.toInt)
    port.foreach(startup)

  private def startup(port: Int): Unit =
    val config = ConfigFactory.parseString(s"""
      pekko.remote.artery.canonical.port = $port
      """).withFallback(ConfigFactory.load())

    val _ = ActorSystem[Nothing](RootBehavior(), "ClusterSystem", config)
