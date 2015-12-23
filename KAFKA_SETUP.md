Kafka Setup - Local Machine
==============================
### Simple Setup 

**File: kafka_2.10-0.8.2.1/config/server.properties**

[Kafka 0.8.0 Docs](http://kafka.apache.org/08/documentation.html)

[Kafka 0.8.2 Docs](http://kafka.apache.org/082/documentation.html)

###### Set the Kafka host address
If you change kafka host name, make sure you use this address in all other places.

`host.name=<private_ip>`

###### Set log retention strategy appropriately
`log.retention.minutes=5`
 
###### Set the log directory if needed to avoid disk space exhaustion:
`log.dirs=/mnt/cass_data_disks/data2/kafka-logs`

### Start Kafka, create the topics and test:

`bin/zookeeper-server-start.sh config/zookeeper.properties`

`bin/kafka-server-start.sh config/server.properties`

**[Kafka 0.8.0](http://kafka.apache.org/08/documentation.html)**

`bin/kafka-create-topic.sh --zookeeper localhost:2181 --replica 1 --partition 20 --topic ratings`

**[Kafka 0.8.2](http://kafka.apache.org/082/documentation.html)**

`bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 20 --topic emails`

`bin/kafka-list-topic.sh --zookeeper localhost:2181 --topic ratings`

`bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic ratings --from-beginning`

Kafka Remote Server Setup
===================================

**Note: Everything is run in the kafka root directory**

### Setup the JMX properties so you can monitor Kafka with JConsole.  

###### Change rmi.server.hostname to your IP address
`export KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=54.153.61.18 -Dcom.sun.management.jmxremote.port=5052"`

###### Every kafka command will require an open JMX Port so you will have to run this with a different port after every command.
`export JMX_PORT=5050`

###### For load testing kafka can quickly run out of disk space.  Modify the server config to delete messages in config/server.properties
`log.retention.minutes=30`
`log.cleanup.policy=delete`
`log.cleaner.enable=true`

`nohup bin/zookeeper-server-start.sh config/zookeeper.properties  2>&1 1> zookeeper.log &`
 
`nohup bin/kafka-server-start.sh config/server.properties 2>&1 1> kafka.log &`

###### On your local machine / laptop run jconsole to monitor kafka.  Enter the IP and Port from above
`jconsole &`

**[Kafka 0.8.0](http://kafka.apache.org/08/documentation.html)**

`bin/kafka-create-topic.sh --zookeeper localhost:2181 --replica 1 --partition 1 --topic ratings`
`bin/kafka-list-topic.sh --zookeeper localhost:2181`

**[Kafka 0.8.2](http://kafka.apache.org/082/documentation.html)**

`bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 5 --topic ratings`
`bin/kafka-topics.sh --list --zookeeper localhost:2181`
`bin/kafka-topics.sh --describe --topic ratings --zookeeper localhost:2181`

`bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic ratings --from-beginning`
`bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic ratings`
