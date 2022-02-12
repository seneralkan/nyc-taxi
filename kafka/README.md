## Start Kafka

1. Start zookeeper
` [train@localhost play]$ sudo systemctl start zookeeper`

2. Start kafka server
` [train@localhost play]$ sudo systemctl start kafka`

# Data Generator and Kafka Producer Consumer

### List Kafka Topics

`kafka-topics.sh --bootstrap-server localhost:9092 --list`

### Delete Kafka Topic

`
kafka-topics.sh --bootstrap-server localhost:9092 \
--delete --topic test1
`

### Create Kafka Topic

#### For Data Engineering Streaming Case
`kafka-topics.sh --bootstrap-server localhost:9092 \
--create --topic taxi \
--replication-factor 1 \
--partitions 2`

#### For ML Streaming
`kafka-topics.sh --bootstrap-server localhost:9092 \
--create --topic taxi-trip-dur-gt700 \
--replication-factor 1 \
--partitions 2`

`kafka-topics.sh --bootstrap-server localhost:9092 \
--create --topic taxi-trip-dur-lt700 \
--replication-factor 1 \
--partitions 2`

### Kafka Producer

`kafka-console-producer.sh \
--bootstrap-server localhost:9092 \
--topic test1`

### Activate Datagen

`source datagen/bin/activate`

### Generate Dataset
`python dataframe_to_kafka.py -i ~/datasets/nyc_taxi_subset.csv -t test1`

### Kafka Consumer
`kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test1 --group taxi_group`

`kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test2 --group taxi_group`
