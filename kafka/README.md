## Start Kafka

1. Start zookeeper
` [train@localhost play]$ sudo systemctl start zookeeper`

2. Start kafka server
` [train@localhost play]$ sudo systemctl start kafka`

## Start Docker
`sudo systemctl start docker`

## Start Elastic Search

Direct your file path to the docker-compose.yml path

`docker-compose up -d`

Wait the installing of the docker images.

Kibana:
`http://localhost:5601`

Elastic Search:
`http://localhost:9200`

1) Open Kibana UI
2) Go to Dev Tools

-- Get All Indexes Command

-- `GET /_cat/indices?v`


-- CREATE INDEX

PUT my-index-000004
{
  "mappings": {
    "properties": {
      "pickup_location": {
        "type": "geo_point"
      },
      "dropoff_location": {
          "type": "geo_point"
      },
      "passenger_count": {
          "type": "long"
      },
      "pickupDayofWeek_TR": {
          "type": "text"
      },
      "pickupMonth_TR": {
          "type": "text"
      },
      "pickup_dayofweek": {
          "type": "long"
      },
      "pickup_hour": {
          "type": "long"
      },
      "pickup_month": {
          "type": "long"
      },
      "pickup_year": {
          "type": "long"
      },
      "store_and_fwd_flag": {
          "type": "text"
      },
      "travel_speed": {
          "type": "long"
      },
      "trip_duration": {
          "type": "long"
        },
        "vendor_id": {
          "type": "long"
      }
    }
  }
}

POST _sql?format=txt
  {
    "query": """
    SELECT * FROM "my-index-000004"
    """
  }


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
---- Data Engineering
`python dataframe_to_kafka.py -i ~/datasets/nyc_taxi_subset.csv -t test1`

---- ML Prediction
`python dataframe_to_kafka.py -i ~/datasets/test.csv -t test1`

### Kafka Consumer
---- Data Engineering
`kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test1 --group taxi_group`

---- ML Prediction
`kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic taxi-trip-dur-gt700`

`kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic taxi-trip-dur-lt700`
