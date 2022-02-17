import findspark
findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
.appName("Read From Kafka")
.config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
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


# lines3 = lines2.withColumn("value2", F.split(F.col("value"), ",")) \
#                 .withColumn("Species", F.trim(F.split(F.col("value"), ",")[4])) \
#                 .withColumn("SepalLengthCm", F.split(F.col("value"), ",")[0]) \
#                 .filter(F.col("Species") == "Iris-setosa")


checkpoint_dir = "file:///tmp/streaming/read_from_kafka"
# write result to console sink
streamingQuery = (lines2
.writeStream
.format("console")
.outputMode("append")
.trigger(processingTime="2 second")
.option("checkpointLocation", checkpoint_dir)
.option("numRows",20)
.option("truncate",False)
.start())


# start streaming
streamingQuery.awaitTermination()