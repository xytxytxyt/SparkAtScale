package sparkAtScale

import java.util.UUID

import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext, Time}
import org.joda.time.DateTime

/** This uses the Kafka Direct introduced in Spark 1.4
  *
  */
object StreamingDirectEmails {

  def main(args: Array[String]) {

    if (args.length < 3) {
      println("first paramteter is kafka broker ")
      println("second param whether to display debug output  (true|false) ")
      println("third param is the checkpoint path  ")
      println("fourth param is the maxRatePerPartition (records/sec to read from each kafka partition)  ")
      println("fifth param is the batch interval in milliseconds")
    }

    val brokers = args(0)
    val debugOutput = args(1).toBoolean
    val checkpoint_path = args(2).toString
    val maxRatePerPartition = args(3).toString
    val batchIntervalInMillis = args(4).toInt

    //val conf = new SparkConf()
    val conf = new SparkConf()
                 .set("spark.streaming.kafka.maxRatePerPartition", maxRatePerPartition)
                 .set("spark.cassandra.connection.keep_alive_ms", (batchIntervalInMillis*5).toString)
    val sc = SparkContext.getOrCreate(conf)

    def createStreamingContext(): StreamingContext = {
      @transient val newSsc = new StreamingContext(sc, Milliseconds(batchIntervalInMillis))
      newSsc.checkpoint(checkpoint_path)
      println(s"Creating new StreamingContext $newSsc with checkpoint path of: $checkpoint_path")
      newSsc
    }

    /*
    val hadoopConf: Configuration = SparkHadoopUtil.get.conf
    hadoopConf.set("cassandra.username", "robot")
    hadoopConf.set("cassandra.password", "silver")
    val ssc = StreamingContext.getActiveOrCreate(checkpoint_path, createStreamingContext, hadoopConf)
    */
    val ssc = StreamingContext.getActiveOrCreate(checkpoint_path, createStreamingContext)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val topics = Set("emails")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    println(s"connecting to brokers: $brokers")
    println(s"ssc: $ssc")
    println(s"kafkaParams: $kafkaParams")
    println(s"topics: $topics")

    val emailsStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    emailsStream.foreachRDD {
      (message: RDD[(String, String)], batchTime: Time) => {
        // convert each RDD from the batch into a Email DataFrame
        //email data has the format msg_id:tenant_id:mailbox_id:time_delivered:time_forwarded:time_read:time_replied
        val df = message.map {
          case (key, nxtEmail) => nxtEmail.split("::")
        }.map(email => {
          val time_delivered: Long = email(3).trim.toLong
          val time_forwarded: Long = email(4).trim.toLong
          val time_read: Long = email(5).trim.toLong
          val time_replied: Long = email(6).trim.toLong
          Email(email(0).trim.toString, email(1).trim.toString, email(2).trim.toString, time_delivered, time_forwarded, time_read, time_replied)
        }).toDF("msg_id", "tenant_id", "mailbox_id", "time_delivered", "time_forwarded", "time_read", "time_replied")

        // this can be used to debug dataframes
        if (debugOutput)
          df.show()

        // save the DataFrame to Cassandra
        // Note:  Cassandra has been initialized through dse spark-submit, so we don't have to explicitly set the connection
        df.write.format("org.apache.spark.sql.cassandra")
          .mode(SaveMode.Append)
          .options(Map("keyspace" -> "email_db", "table" -> "email_msg_tracker"))
          .save()
        
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
