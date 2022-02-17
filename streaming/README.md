# Start Kafka

1. Start zookeeper
` [train@localhost play]$ sudo systemctl start zookeeper`

2. Start kafka server
` [train@localhost play]$ sudo systemctl start kafka`

# Start Docker
`sudo systemctl start docker`

# Start Elastic Search

1. Direct your file path to the docker-compose.yml path

`docker-compose up -d`

2. Wait the installing of the docker images.

3. Kibana:
`http://localhost:5601`

4. Elastic Search:
`http://localhost:9200`

5. Open Kibana UI
6. Go to Dev Tools

### Get All Indexes Command

`GET /_cat/indices?v`


### Create Index in Elastic Search

`  PUT my-index-000008
{
  "mappings": {
    "properties": {
      "pickup_location": {
        "type": "geo_point"
      },
      "dropoff_location": {
          "type": "geo_point"
      },
      "pickup_datetime":{
          "type": "date"
      },
      "dropoff_datetime":{
          "type": "date"
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
}`

#### Check Index
`POST _sql?format=txt
  {
    "query": """
    SELECT * FROM "my-index-000004"
    """
  }`


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
