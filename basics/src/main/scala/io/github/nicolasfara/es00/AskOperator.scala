package io.github.nicolasfara.es00

import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.scaladsl.*
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.util.Timeout

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.*

object GreeterActor:
	enum Command:
		case Greet(name: String, replyTo: ActorRef[Greeting])

	final case class Greeting(message: String)

	def apply(): Behavior[Command] = Behaviors.receiveMessage:
		case Command.Greet(name, replyTo) =>
			replyTo ! Greeting(s"Hello, $name!")
			Behaviors.same

@main def runAskOperator(): Unit =
	given Timeout = 3.seconds

	val system = ActorSystem(GreeterActor(), "AskExample")
	given ActorSystem[GreeterActor.Command] = system

	val greeting: Future[GreeterActor.Greeting] = system ? { replyTo =>
		GreeterActor.Command.Greet("Pekko", replyTo)
	}

	val result = Await.result(greeting, 3.seconds)
	println(result.message)

	system.terminate()


