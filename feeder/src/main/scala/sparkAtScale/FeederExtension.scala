package sparkAtScale

import java.util.Properties

import akka.actor._
import org.apache.kafka.clients.producer.{KafkaProducer,ProducerConfig}

object FeederExtension extends ExtensionKey[FeederExtension]

class FeederExtension(system: ExtendedActorSystem) extends Extension {

  val systemConfig = system.settings.config

  val file = systemConfig.getString("sparkAtScale.file")
  val movie_ids_file = systemConfig.getString("sparkAtScale.movie_ids_file")
  val kafkaHost = systemConfig.getString("sparkAtScale.kafkaHost")
  println(s"kafkaHost $kafkaHost")
  val kafkaTopic = systemConfig.getString("sparkAtScale.kafkaTopic")
  val numKafkaKeys= systemConfig.getInt("sparkAtScale.numKafkaKeys")
  println(s"numKafkaKeys $numKafkaKeys")
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

}

trait FeederExtensionActor { this: Actor =>
  val feederExtension: FeederExtension = FeederExtension(context.system)
}
