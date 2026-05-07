package io.github.nicolasfara.es01.cluster.router

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.typed.*

def createActorSystem[A](port: Int, behavior: Behavior[A]) =
  val config = ConfigFactory.parseString(s"""
    pekko.remote.artery.canonical.port = $port
    """).withFallback(ConfigFactory.load("application-router.conf"))

  ActorSystem[A](behavior, "ClusterSystem", config)
