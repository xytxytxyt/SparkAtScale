package sparkAtScale

import akka.actor.{Actor, ActorLogging, Props}
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}
import org.joda.time.DateTime

import scala.concurrent.duration.{Duration, _}
import scala.util.Random
import java.util.UUID

case class Email(
                  msg_id: String,
                  tenant_id: UUID,
                  mailbox_id: UUID,
                  time_delivered: Long,
                  time_forwarded: Long,
                  time_read: Long,
                  time_replied: Long) {

  override def toString: String = {
    s"$msg_id::$tenant_id::$mailbox_id::$time_delivered::$time_forwarded::$time_read::$time_replied"
  }
}

/**
 * Randomly generates email tracker data and sends to Kafka.
 */
class RandomFeederActor(tickInterval:FiniteDuration) extends Actor with ActorLogging with FeederExtensionActor {

  log.info(s"Starting random feeder actor ${self.path}")

  import FeederActor.SendNextLine

  var counter = 0

  implicit val executionContext = context.system.dispatcher

  val feederTick = context.system.scheduler.schedule(Duration.Zero, tickInterval, self, SendNextLine)

  val randMovies = Random
  //var movieIds: Array[Int] = initData()
  //val idLength = movieIds.length

  //val randUser = Random
  val randGen = Random
  val dateTime = new DateTime(2000,1,1,0,0,0,0)

  //pick out of 15 million random users
  //val userLength = 15000000

  //val randEmail = Random
  //val randEmailDecimal = Random

  var emailsSent = 0

  def receive = {
    case SendNextLine =>

      val numPartitions = 1000 // follow-up needed: constraining this for now, but we'll want to generalize this
      val nxtMessageId = "messageId-"+randGen.nextInt(numPartitions)
      // follow-up needed: keeping this fixed for now to make querying easier
      val nxtTenantId = UUID.fromString("9b657ca1-bfb1-49c0-85f5-04b127adc6f3") 
      val nxtMailboxId = UUID.randomUUID()
      val nxtTimeDelivered = dateTime.plusSeconds(randGen.nextInt()).getMillis
      val nxtTimeForwarded = dateTime.plusSeconds(randGen.nextInt()).getMillis
      val nxtTimeRead = dateTime.plusSeconds(randGen.nextInt()).getMillis
      val nxtTimeReplied = dateTime.plusSeconds(randGen.nextInt()).getMillis

      val nxtEmail = Email(nxtMessageId, nxtTenantId, nxtMailboxId, nxtTimeDelivered, nxtTimeForwarded, nxtTimeRead, nxtTimeReplied)

      emailsSent += 1

      //email data has the format msg_id:tenant_id:mailbox_id:time_delivered:time_forwarded:time_read:time_replied
      //the key for the producer record is ((msg_id, tenant_id), mailbox_id)
      val key = s"${nxtEmail.msg_id}${nxtEmail.tenant_id}${nxtEmail.mailbox_id}"
      
      //val record = new ProducerRecord[String, String](feederExtension.kafkaTopic, key, nxtEmail.toString)
      val record = new ProducerRecord[String, String](feederExtension.kafkaTopic, key, nxtEmail.toString)

      val future = feederExtension.producer.send(record, new Callback {
        override def onCompletion(result: RecordMetadata, exception: Exception) {
          if (exception != null) log.info("Failed to send record: " + exception)
          else {
            //periodically log the num of messages sent
            if (emailsSent % 20987 == 0)
              log.info(s"emailsSent = $emailsSent  //  result partition: ${result.partition()}")
          }
        }
      })

    // Use future.get() to make this a synchronous write
  }

  def initData() = {
    val source = scala.io.Source.fromFile(feederExtension.movie_ids_file).getLines()
    val movieIdsStrs: Array[String] = source.next().split(", ")
    movieIdsStrs.collect {
      case nxtIdStr if nxtIdStr.length > 0 => nxtIdStr.toInt
    }
  }
}

object RandomFeederActor {
  def props(tickInterval:FiniteDuration) = Props(new RandomFeederActor(tickInterval))
  case object ShutDown

  case object SendNextLine

}
