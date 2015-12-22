# Spark at Scale
 
This demo simulates a stream of email metadata.  Data flows from akka -> kafka -> spark streaming -> cassandra

### Kafka Setup 

[See the Kafka Setup Instructions in the KAFKA_SETUP.md file](KAFKA_SETUP.md)

### Setup the KS/table

`cqlsh -f /path_to_SparkAtScale/LoadMovieData/conf/movie_db.cql` 


### Setup Akka Feeder

###### Build the feeder fat jar   
`sbt feeder/assembly`

Edit kafkaHost and kafkaTopic if needed. kafaHost should match the setting in kafka/conf/server.properties and kafkaTopic should match that used when creating the topic.

###### Run the feeder

Parameters:
1. Number of feeders to start 
2. Time interval (ms) between sent requests by each feeder (1 feeder sending a message every 100 ms will equate to 10 messages/sec)
3. Feeder name
`java -Xmx5g -Dconfig.file=dev.conf -jar feeder/target/scala-2.10/feeder-assembly-1.0.jar 1 100 emailFeeder 2>&1 1>feeder-out.log &`


### Run Spark Streaming

###### Build the streaming jar
`sbt streaming/package`

Note: You will want to reference the correct Spark version, for example running against Spark 1.4 use 1.4.1 instead of 1.5.0

Parameters:
1. kafka broker: Ex. 10.200.185.103:9092 
2. debug flag (limited use): Ex. true or false 
3. checkpoint directory name: Ex. cfs://10.200.162.82/emails_checkpoint, dsefs://10.200.162.82/emails_checkpoint
4. [spark.streaming.kafka.maxRatePerPartition](http://spark.apache.org/docs/latest/configuration.html#spark-streaming): Maximum rate (number of records per second) 
5. batch interval (ms) 
6. [auto.offset.reset](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.kafka.KafkaUtils$): Ex. smallest or largest

###### Running locally for development
`spark-submit --packages org.apache.spark:spark-streaming-kafka-assembly_2.10:1.5.0 --class sparkAtScale.StreamingDirectEmails streaming/target/scala-2.10/streaming_2.10-1.0.jar 10.200.162.82:9092 true dsefs://10.200.162.82/emails_checkpoint 50000 5000 largest`
 
###### Running on a server in foreground
`dse spark-submit --driver-memory 2G --packages org.apache.spark:spark-streaming-kafka-assembly_2.10:1.5.0 --class sparkAtScale.StreamingDirectEmls ./streaming/target/scala-2.10/streaming_2.10-0.1.jar 10.200.162.82:9092 true dsefs://10.200.162.82/emails_checkpoint 50000 5000 largest`
 
###### Running on the server for production mode
`nohup dse spark-submit --driver-memory 2G --packages org.apache.spark:spark-streaming-kafka-assembly_2.10:1.5.0 --class sparkAtScale.StreamingDirectEmls ./streaming/target/scala-2.10/streaming_2.10-0.1.jar 10.200.162.82:9092 true dsefs://10.200.162.82/emails_checkpoint 50000 5000 largest >& streaming.out &`
