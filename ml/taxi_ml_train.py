import findspark
findspark.init("/opt/manual/spark/")
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, FloatType
from math import radians, cos, sin, asin, sqrt
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.regression import DecisionTreeRegressor, LinearRegression
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.functions import date_format, sin, cos, radians, atan2, month
from pyspark.ml.feature import VectorAssembler, VectorIndexer
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler

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


spark = SparkSession.builder \
.appName("ML Homework") \
.master("local[2]") \
.getOrCreate()

haversine_distance = F.udf(lambda lon1, lat1, lon2, lat2: haversine(lon1, lat1, lon2, lat2), FloatType())
spark.udf.register("haversine_distance", haversine_distance)

switch_month = F.udf(lambda z: switch_month_day(z), StringType())
spark.udf.register("switch_month", switch_month)

switch_day_func = F.udf(lambda z: switch_tr_day(z), StringType())
spark.udf.register("switch_day_func", switch_day_func)

df = spark.read.format("csv") \
.option("header",True) \
.option("inferSchema",True) \
.option("sep",",") \
.load("file:///home/train/datasets/nyc_taxi.csv")

test = spark.read.format("csv") \
.option("header",True) \
.option("inferSchema",True) \
.option("sep",",") \
.load("file:///home/train/datasets/test.csv")

# Preparation Train Dataset
df1 = df.withColumn("pickup_datetime", F.to_timestamp(F.col("pickup_datetime"),"yyyy-MM-dd HH:mm:ss")) \
        .withColumn("dropoff_datetime", F.to_timestamp(F.col("dropoff_datetime"),"yyyy-MM-dd HH:mm:ss"))

df2 = df1 \
       .withColumn("pickup_year",
                    F.year(F.to_date(F.col("pickup_datetime")))) \
       .withColumn("pickup_month",
                    F.month(F.to_date(F.col("pickup_datetime")))) \
       .withColumn("pickup_dayofweek",
                    F.dayofweek(F.to_date(F.col("pickup_datetime")))) \
       .withColumn("pickup_hour",
                    F.hour(F.col("pickup_datetime")))

df3 = df2.withColumn("id", F.regexp_replace(F.col("id"), "[id]", "")) \
.withColumn("haversine_distance_km", haversine_distance(F.col("pickup_longitude"), F.col("pickup_latitude"), F.col("dropoff_longitude"), F.col("dropoff_latitude"))) \
.drop("store_and_fwd_flag", "pickup_year", "dropoff_datetime", "pickup_datetime", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude", "id")

df4 = df3.withColumnRenamed("trip_duration", "label")

assembler = VectorAssembler(inputCols=['vendor_id', 'passenger_count', 'pickup_month', 'pickup_dayofweek',
            'pickup_hour', 'haversine_distance_km'], outputCol='features', 
            handleInvalid='skip')

df5 = assembler.setHandleInvalid("skip").transform(df4).select("label", "features")

# Preparation Test Dataset
test1 = test.withColumn("pickup_datetime", F.to_timestamp(F.col("pickup_datetime"),"yyyy-MM-dd HH:mm:ss"))

test2 = test1 \
       .withColumn("pickup_year",
                    F.year(F.to_date(F.col("pickup_datetime")))) \
       .withColumn("pickup_month",
                    F.month(F.to_date(F.col("pickup_datetime")))) \
       .withColumn("pickup_dayofweek",
                    F.dayofweek(F.to_date(F.col("pickup_datetime")))) \
       .withColumn("pickup_hour",
                    F.hour(F.col("pickup_datetime")))

testdf = test2.withColumn("id", F.regexp_replace(F.col("id"), "[id]", "")) \
.withColumn("haversine_distance_km", haversine_distance(F.col("pickup_longitude"), F.col("pickup_latitude"), F.col("dropoff_longitude"), F.col("dropoff_latitude"))) \
.drop("store_and_fwd_flag", "pickup_year", "pickup_datetime", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude", "id")


# Model
dtr = DecisionTreeRegressor(featuresCol="features", labelCol="label", impurity="variance")

# Choices of Tuning Parameters
dtrparamGrid = (ParamGridBuilder().addGrid(dtr.maxDepth, [10]).build())

pipeline = Pipeline(stages = [assembler, dtr])

crossval = CrossValidator(estimator = pipeline, estimatorParamMaps = dtrparamGrid, evaluator = RegressionEvaluator(labelCol = "label", predictionCol = "prediction", metricName = "rmse"), numFolds = 10)
model = crossval.fit(df4)

predictions = model.transform(testdf).cache()
predictions.show(25)

# Evaulation
evaluator = RegressionEvaluator(labelCol="lable", predictionCol = "prediction", metricName = "rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

evaluator2 = RegressionEvaluator(labelCol="label", predictionCol = "prediction", metricName = "mae")
mae = evaluator2.evaluate(predictions)
print("Mean Absolute Error (MAE) on test data = %g" % mae)

# Write Model
model.write().overwrite().save("file:///home/train/project/nyc_taxi_model")

# Load the model
from pyspark.ml.tuning import TrainValidationSplitModel

loaded_pipeline_model = TrainValidationSplitModel.load("file:///home/train/project/nyc_taxi_model")

final_df = loaded_pipeline_model.transform(testdf)
final_df= final_df.drop("assembled_features","features")