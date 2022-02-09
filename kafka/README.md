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

`kafka-topics.sh --bootstrap-server localhost:9092 \
--create --topic taxi \
--replication-factor 1 \
--partitions 2`

### Kafka Producer

`kafka-console-producer.sh \
--bootstrap-server localhost:9092 \
--topic taxi`

### Activate Datagen

`source datagen/bin/activate`

### Generate Dataset
`python dataframe_to_kafka.py -i ~/datasets/nyc_taxi.csv -t taxi`

`python dataframe_to_kafka.py -i ~/datasets/iris.csv -t test1 -k 4`

### Kafka Consumer
`kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test1 --group taxi_group`

`kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test2 --group iris_group`
