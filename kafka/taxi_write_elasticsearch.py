import findspark
findspark.init("/opt/manual/spark")

from elasticsearch import Elasticsearch, helpers
from pyspark.sql import SparkSession, functions as F
import time

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
                .withColumn("Species", F.trim(F.split(F.col("value"), ",")[4])) \
                .withColumn("SepalLengthCm", F.split(F.col("value"), ",")[0]) \
                .filter(F.col("Species") == "Iris-setosa")

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
    # df.write \
    # .format("org.elasticsearch.spark.sql") \
    # .mode("append") \
    # .option("es.nodes", "localhost") \
    # .option("es.port","9200") \
    # .save("iris")

    df.unpersist()

checkpoint_dir = "file:///tmp/streaming/read_from_kafka"
# start streaming
streamingQuery = (lines3
                  .writeStream
                  .foreachBatch(write_to_multiple_sinks)
                  .trigger(processingTime="1 second")
                  .option("checkpointLocation", checkpointDir)
                  .start())

# start streaming
streamingQuery.awaitTermination()