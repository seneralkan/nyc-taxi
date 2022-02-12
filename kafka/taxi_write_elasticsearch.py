import findspark
findspark.init("/opt/manual/spark")

from elasticsearch import Elasticsearch, helpers
from pyspark.sql import SparkSession, functions as F
import time
from helpers import switch_month_day, switch_tr_day, haversine

spark = (SparkSession.builder
.appName("Read From Kafka")
.master("local[2]")
.config("spark.driver.memory","2048m")
.config("spark.sql.shuffle.partitions", 4)
.config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
.config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:7.12.1") 
.getOrCreate())

spark.sparkContext.setLogLevel('ERROR')

# Register UDF Fuctions
haversine_distance = F.udf(lambda lon1, lat1, lon2, lat2: haversine(lon1, lat1, lon2, lat2), FloatType())
spark.udf.register("haversine_distance", haversine_distance)

switch_month = F.udf(lambda z: switch_month_day(z), StringType())
spark.udf.register("switch_month", switch_month)

switch_tr = F.udf(lambda z: switch_tr_day(z), StringType())
spark.udf.register("switch_tr", switch_tr)


# Read data from kafka source
lines = (spark
.readStream
.format("kafka")
.option("kafka.bootstrap.servers", "localhost:9092")
.option("subscribe", "test1")
.load())

# # deserialize key and value
lines2 = lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)",
                          "topic", "partition", "offset", "timestamp")


lines3 = lines2.withColumn("value2", F.split(F.col("value"), ",")) \
                .withColumn("id", F.trim(F.split(F.col("value"), ",")[0])) \
                .withColumn("vendor_id", F.split(F.col("value"), ",")[1]) \
                .withColumn("pickup_datetime", F.split(F.col("value"), ",")[2]) \
                .withColumn("dropoff_datetime", F.split(F.col("value"), ",")[3]) \
                .withColumn("passenger_count", F.split(F.col("value"), ",")[4]) \
                .withColumn("pickup_longitude", F.split(F.col("value"), ",")[5]) \
                .withColumn("pickup_latitude", F.split(F.col("value"), ",")[6]) \
                .withColumn("dropoff_longitude", F.split(F.col("value"), ",")[7]) \
                .withColumn("dropoff_latitude", F.split(F.col("value"), ",")[8]) \
                .withColumn("store_and_fwd_flag", F.split(F.col("value"), ",")[9]) \
                .withColumn("trip_duration", F.split(F.col("value"), ",")[10]) \

# Transformation
lines4 = lines3.withColumn("pickup_datetime",
                        F.to_timestamp(F.col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("dropoff_datetime",
                        F.to_timestamp(F.col("dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"))

lines5 = lines4.withColumn("pickup_year",
                        F.year(F.to_date(F.col("pickup_datetime")))) \
            .withColumn("pickup_month",
                        F.month(F.to_date(F.col("pickup_datetime")))) \
            .withColumn("pickup_dayofweek",
                        F.dayofweek(F.to_date(F.col("pickup_datetime")))) \
            .withColumn("pickup_hour",
                        F.hour(F.col("pickup_datetime"))) \
            .withColumn("pickupDayofWeek_TR",
                        switch_tr(F.col("pickup_dayofweek"))) \
            .withColumn("pickupMonth_TR",
                        switch_month(F.col("pickup_month"))) \
            .withColumn("haversine_distance(km)",
                        haversine_distance(F.col("pickup_longitude"), F.col("pickup_latitude"),
                                           F.col("dropoff_longitude"),
                                           F.col("dropoff_latitude"))) \
            .withColumn("travel_speed", 
                        1000 * F.col("haversine_distance(km)") / F.col("trip_duration")) \
            .drop("pickup_datetime", "dropoff_datetime")


jdbcUrl = "jdbc:postgresql://localhost/traindb?user=train&password=Ankara06"

def write_to_multiple_sinks(df, batchId):
    df.persist()
    df.show()

    # write postgresql
    df.write.jdbc(url=jdbcUrl,
                  table="iris",
                  mode="append",
                  properties={"driver": 'org.postgresql.Driver'})

    # write to elasticsearch
    df.write \
    .format("org.elasticsearch.spark.sql") \
    .mode("append") \
    .option("es.nodes", "localhost") \
    .option("es.port","9200") \
    .save("taxi")

    df.unpersist()

checkpoint_dir = "file:///tmp/streaming/read_from_kafka"
# start streaming
streamingQuery = (lines5
                  .writeStream
                  .foreachBatch(write_to_multiple_sinks)
                  .trigger(processingTime="1 second")
                  .option("checkpointLocation", checkpointDir)
                  .start())

# start streaming
streamingQuery.awaitTermination()