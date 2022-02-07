import findspark
from pyspark.sql import SparkSession, functions as F
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 350)

findspark.init("/opt/manual/spark")

spark = SparkSession.builder \
.appName("Filter") \
.master("local[2]") \
.getOrCreate()
#müthiş
def read_data(GitHub_Repository_URL):
    df = spark.read.option("header", True) \
    .option("inferSchema", True) \
    .option("maxColumns", 100000) \
    .csv(GitHub_Repository_URL)
    df.printSchema()
    print(df.limit(5).toPandas())
    return df

df_train = read_data("file:///home/train/datasets/nyc/train.csv")
df_test = read_data("file:///home/train/datasets/nyc/test.csv")

