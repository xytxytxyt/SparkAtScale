# Stateful Network Word Count Example
`dse spark-submit --class org.apache.spark.examples.streaming.StatefulNetworkWordCount ./examples/target/scala-2.10/examples-assembly-0.1.jar localhost 9999`

# Kafka Word Count Example
`dse spark-submit --class org.apache.spark.examples.streaming.KafkaWordCount ./examples/target/scala-2.10/examples-assembly-0.1.jar localhost:2181 test-consumer-group emails 2 dsefs:///kafkawordcount`
