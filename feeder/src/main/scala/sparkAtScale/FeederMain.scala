package sparkAtScale

import akka.actor.{Props, ActorSystem}

import scala.concurrent.duration.{FiniteDuration, Duration, MILLISECONDS}

object FeederMain {

  def main(args: Array[String]) {

    if (args.length < 3) {
      println("First argument is the number of feeders to start")
      println("Second argument is the time in Milliseconds to generate events in each actor")
      println("Third argument is set to (ratingFeeder|randRatingsFeeder|emailFeeder) to generate ratings randomly")
      System.exit(0)
    }
    val numFeeders = args(0).toInt

    val system = ActorSystem("MyActorSystem")

    val tickDuration: FiniteDuration = Duration.create(args(1).toLong, MILLISECONDS)
    println(s"tick duration: ${tickDuration}")


    val feederActorProps = args(2) match {
      case "ratingFeeder" => FeederActor.props(tickDuration)
      case "randRatingFeeder" => RandomFeederActor.props(tickDuration)
      case "emailFeeder" => EmailFeederActor.props(tickDuration)
    }
    
    /*
    val feederActorProps = if (args(2).toBoolean) {
      RandomFeederActor.props(tickDuration)
    }
    else {
      FeederActor.props(tickDuration)
    }
    */

    for (indx <- 1 to numFeeders) {
      val feederActor = system.actorOf(feederActorProps, s"feederActor-$indx")
    }

    system.awaitTermination()

  }

}
