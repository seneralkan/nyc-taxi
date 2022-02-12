import findspark
findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.types import StringType, FloatType, IntegerType, DateType, TimestampType
from pyspark.ml.pipeline import PipelineModel
from helpers import switch_month_day, switch_tr_day, haversine

spark = (SparkSession.builder
.appName("Write to Kafka")
.config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
.getOrCreate())

spark.sparkContext.setLogLevel('ERROR')
checkpoint_dir = "file:///tmp/streaming/output"

# Register UDF Functions
haversine_distance = F.udf(lambda lon1, lat1, lon2, lat2: haversine(lon1, lat1, lon2, lat2), FloatType())
spark.udf.register("haversine_distance", haversine_distance)

switch_month = F.udf(lambda z: switch_month_day(z), StringType())
spark.udf.register("switch_month", switch_month)

switch_day_func = F.udf(lambda z: switch_tr_day(z), StringType())
spark.udf.register("switch_day_func", switch_day_func)

# Read Streaming Dataset
lines = (spark
.readStream
.format("kafka")
.option("kafka.bootstrap.servers", "localhost:9092")
.option("subscribe", "test1")
.load())


# Deserialize Key and Value
lines2 = lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)",
                          "topic", "partition", "offset", "timestamp")

lines3 = lines2.withColumn("value2", F.split(F.col("value"), ",")) \
                .withColumn("id", F.trim(F.split(F.col("value"), ",")[0])) \
                .withColumn("vendor_id", F.split(F.col("value"), ",")[1]) \
                .withColumn("pickup_datetime", F.split(F.col("value"), ",")[2]) \
                .withColumn("passenger_count", F.split(F.col("value"), ",")[3]) \
                .withColumn("pickup_longitude", F.split(F.col("value"), ",")[4]) \
                .withColumn("pickup_latitude", F.split(F.col("value"), ",")[5]) \
                .withColumn("dropoff_longitude", F.split(F.col("value"), ",")[6]) \
                .withColumn("dropoff_latitude", F.split(F.col("value"), ",")[7]) \
                .drop("value")

# Cast Columns
lines4 = lines3.withColumn("id", F.col("id"), F.col("id").cast(StringType())) \
                .withColumn("vendor_id", F.col("vendor_id").cast(IntegerType())) \
                .withColumn("pickup_datetime", F.col("pickup_datetime").cast(StringType())) \
                .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType())) \
                .withColumn("pickup_longitude", F.col("pickup_longitude").cast(FloatType)) \
                .withColumn("pickup_latitude", F.col("pickup_latitude").cast(FloatType())) \
                .withColumn("dropoff_longitude", F.col("dropoff_longitude").cast(FloatType())) \
                .withColumn("dropoff_latitude", F.col("dropoff_latitude").cast(FloatType()))

# Transformation Test Dataset for Predicting the Model
lines4 = lines3.withColumn("pickup_datetime", 
                    F.to_timestamp(F.col("pickup_datetime"),"yyyy-MM-dd HH:mm:ss")) \
            .withColumn("pickup_month",
                    F.month(F.to_date(F.col("pickup_datetime")))) \
            .withColumn("pickup_dayofweek",
                    F.dayofweek(F.to_date(F.col("pickup_datetime")))) \
            .withColumn("pickup_hour",
                    F.hour(F.col("pickup_datetime")))

lines5= lines4.withColumn("haversine_distance_km", haversine_distance(F.col("pickup_longitude"), F.col("pickup_latitude"), F.col("dropoff_longitude"), F.col("dropoff_latitude"))) \
.drop("pickup_datetime", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude", "id")


# Laod Pipeline Model
loaded_pipeline_model = PipelineModel.load("file:///home/train/project/nyc_taxi_model")

loaded_nolabel_model = PipelineModel.load("file:///home/train/project/nyc_taxi_model_nolabel_model")

# Select columns in order to send Kafka
cols = ["pickup_month", "pickup_dayofweek", "pickup_hour", "vendor_id", "haversine_distance_km", "prediction"]

def predict(df, batchId):
    
    prepare_df = loaded_nolabel_model.transform(df)

    transformed_df = loaded_pipeline_model.stages[-1].transform(prepare_df)

    transformed_df.show(5, truncate=False)
    
    # Serialization and Filtering Predicted Topic
    transformed_df.filter("prediction => 700").withColumn("value", F.concat_ws(",", *cols)) \
        .write.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "taxi-trip-dur-gt700") \
        .save()

    transformed_df.filter("prediction =< 700").withColumn("value", F.concat_ws(",", *cols)) \
        .write.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "taxi-trip-dur-lt700") \
        .save()

checkpoint_dir = "file:///tmp/streaming/write_to_kafka"

streamingQuery = (lines5
.writeStream
.foreachBatch(predict)
.option("checkpointLocation", checkpoint_dir)
.start())

streamingQuery.awaitTermination()