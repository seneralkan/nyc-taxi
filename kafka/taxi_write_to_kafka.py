import findspark
findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.types import FloatType

def switch_tr_day(day_index):
    my_dict = {
        1: 'Pazar',
        2: 'Pazartesi',
        3: 'Salı',
        4: 'Çarşamba',
        5: 'Perşembe',
        6: 'Cuma',
        7: 'Cumartesi'
    }
    
    return my_dict.get(day_index)

def switch_month_day(month_index):
    my_dict = {
        1: 'Ocak',
        2: 'Subat',
        3: 'Mart',
        4: 'Nisan',
        5: 'Mayis',
        6: 'Haziran',
        7: 'Temmuz',
        8: 'Agustos',
        9: 'Eylul',
        10: 'Ekim',
        11: 'Kasim',
        12: 'Aralik'
    }
    
    return my_dict.get(month_index)

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance in kilometers between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
    return c * r

spark = (SparkSession.builder
.appName("Write to Kafka")
.config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
.getOrCreate())

spark.sparkContext.setLogLevel('ERROR')
checkpoint_dir = "file:///tmp/streaming/output"

haversine_distance = F.udf(lambda lon1, lat1, lon2, lat2: haversine(lon1, lat1, lon2, lat2), FloatType())
spark.udf.register("haversine_distance", haversine_distance)

switch_month = F.udf(lambda z: switch_month_day(z), StringType())
spark.udf.register("switch_month", switch_month)

switch_day_func = F.udf(lambda z: switch_tr_day(z), StringType())
spark.udf.register("switch_day_func", switch_day_func)

lines = (spark
.readStream
.format("kafka")
.option("kafka.bootstrap.servers", "localhost:9092")
.option("subscribe", "test1")
.load())


# deserialize key and value
lines2 = lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)",
                          "topic", "partition", "offset", "timestamp")

lines3 = lines2.withColumn("value2", F.split(F.col("value"), ",")) \
                .withColumn("id", F.trim(F.split(F.col("value"), ",")[0])) \
                .withColumn("vendor_id", F.split(F.col("value"), ",")[1]) \
                .withColumn("pickup_datetime", F.split(F.col("value"), ",")[2]) \
                .drop("value")

# Transformation
lines4 = lines3.withColumn("pickup_datetime", 
                    F.to_timestamp(F.col("pickup_datetime"),"yyyy-MM-dd HH:mm:ss")) \
            .withColumn("pickup_year",
                    F.year(F.to_date(F.col("pickup_datetime")))) \
            .withColumn("pickup_month",
                    F.month(F.to_date(F.col("pickup_datetime")))) \
            .withColumn("pickup_dayofweek",
                    F.dayofweek(F.to_date(F.col("pickup_datetime")))) \
            .withColumn("pickup_hour",
                    F.hour(F.col("pickup_datetime")))

# Serialization
lines5 = lines4.withColumn("value", F.concat(F.col("pickup_datetime"), F.lit(","), F.col("pickup_year"), \
        F.col("pickup_month"), F.lit(","), F.col("pickup_dayofweek"), \
        F.col("pickup_hour"))) \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

streamingQuery = (lines5
.writeStream
.format("kafka")
.option("kafka.bootstrap.servers", "localhost:9092")
.option("topic", "test2")
.outputMode("append")
.option("checkpointLocation", checkpoint_dir)
.start())

streamingQuery.awaitTermination()