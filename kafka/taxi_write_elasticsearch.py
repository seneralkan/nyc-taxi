import findspark
findspark.init("/opt/manual/spark")

from elasticsearch import Elasticsearch, helpers
from pyspark.sql import SparkSession, functions as F
import time
from helpers import switch_month_day, switch_tr_day, haversine
from pyspark.sql.types import StringType, FloatType, IntegerType, DateType, TimestampType, DoubleType, ArrayType
from math import radians, cos, sin, asin, sqrt

spark = (SparkSession.builder
.appName("Read From Kafka")
.config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:7.12.1," "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
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

# Deserialize key and value
lines2 = lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)",
                          "topic", "partition", "offset", "timestamp")

# Split value and assig to the column
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

# Drop unnecessary Kafka key, value, topic, partition, offset and timestamp colum
lines4 = lines3.drop("key", "value", "value2", "topic", "partition", "offset", "timestamp")

# Check the Schema
lines4.printSchema()

#  |-- id: string (nullable = true)
#  |-- vendor_id: string (nullable = true)
#  |-- pickup_datetime: string (nullable = true)
#  |-- dropoff_datetime: string (nullable = true)
#  |-- passenger_count: string (nullable = true)
#  |-- pickup_longitude: string (nullable = true)
#  |-- pickup_latitude: string (nullable = true)
#  |-- dropoff_longitude: string (nullable = true)
#  |-- dropoff_latitude: string (nullable = true)
#  |-- store_and_fwd_flag: string (nullable = true)
#  |-- trip_duration: string (nullable = true)

def write_to_elasticsearch(df, batchId):
    # Cast columns
    df2 = df.withColumn("id", F.col("id")) \
                .withColumn("vendor_id", F.col("vendor_id").cast(IntegerType())) \
                .withColumn("pickup_datetime", F.col("pickup_datetime")) \
                .withColumn("dropoff_datetime", F.col("dropoff_datetime")) \
                .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType())) \
                .withColumn("pickup_longitude", F.col("pickup_longitude").cast(DoubleType())) \
                .withColumn("pickup_latitude", F.col("pickup_latitude").cast(DoubleType())) \
                .withColumn("dropoff_longitude", F.col("dropoff_longitude").cast(DoubleType())) \
                .withColumn("dropoff_latitude", F.col("dropoff_latitude").cast(DoubleType())) \
                .withColumn("store_and_fwd_flag", F.col("store_and_fwd_flag")) \
                .withColumn("trip_duration", F.col("trip_duration").cast(IntegerType())) \

    # Transformation 1
    df3 = df2.withColumn("pickup_datetime",
                        F.to_timestamp(F.col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("dropoff_datetime",
                        F.to_timestamp(F.col("dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"))

    # Transformation 2
    df4 = df3.withColumn("pickup_year",
                        F.year(F.to_date(F.col("pickup_datetime")))) \
            .withColumn("pickup_month",
                        F.month(F.to_date(F.col("pickup_datetime")))) \
            .withColumn("pickup_dayofweek",
                        F.dayofweek(F.to_date(F.col("pickup_datetime")))) \
            .withColumn("pickup_hour",
                        F.hour(F.col("pickup_datetime")))
    # Transformation 3
    df5 = df4.withColumn("pickupDayofWeek_TR",
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
    
    # Transformation 4 ( Defining lat and lon as an array in order to see as geometric object in Elasticsearch )
    pickup_geo_col = [F.col("pickup_latitude"), F.col("pickup_longitude")]
    dropoff_geo_col = [F.col("dropoff_latitude"), F.col("dropoff_longitude")]

    df6 = df5.withColumn("pickup_location", 
                        F.array(pickup_geo_col)) \
            .withColumn("dropoff_location",
                        F.array(dropoff_geo_col)) \
            .drop("pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude")
    
    df6.show(n=5, truncate=False)

    # Write to Elasticsearch
    df6.write \
    .format("org.elasticsearch.spark.sql") \
    .mode("overwrite") \
    .option("es.nodes", "localhost") \
    .option("es.port","9200") \
    .save("my-index-000004")

checkpoint_dir = "file:///tmp/streaming/read_from_kafka"

####### Write to Elasticsearch #######
# start streaming
streamingQuery = (lines4
                  .writeStream
                  .foreachBatch(write_to_elasticsearch)
                  .trigger(processingTime="10 second")
                  .option("checkpointLocation", checkpoint_dir)
                  .start())

####### Write to Console #######
# streamingQuery = (lines4
# .writeStream
# .format("console")
# .outputMode("append")
# .trigger(processingTime="20 second")
# .option("checkpointLocation", checkpoint_dir)
# .option("numRows",30)
# .option("truncate",True)
# .start())

# start streaming
streamingQuery.awaitTermination()