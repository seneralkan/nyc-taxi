## Start Kafka

1. Start zookeeper
` [train@localhost play]$ sudo systemctl start zookeeper`

2. Start kafka server
` [train@localhost play]$ sudo systemctl start kafka`

# Data Generator and Kafka Producer Consumer

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
`python dataframe_to_kafka.py -i ~/datasets/nyc_taxi.csv -t taxi -k 1`

### Kafka Consumer
`kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test2 --group taxi_group`
