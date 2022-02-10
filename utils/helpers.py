from pyspark.sql import SparkSession, functions as F
from pyspark.sql import DataFrame


class MyHelpers:

    def get_spark_session(self, session_params: dict) -> SparkSession:
        ## Put your code here.
        spark = (SparkSession.builder
                 .appName("Read From Kafka")
                 .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
                 .getOrCreate())
                #TODO: ELASTIC CONFIG EKLENECEK
        spark.sparkContext.setLogLevel('ERROR')
        return spark

    def write_to_elastic(self, input_df: DataFrame): #TODO: GÜNCELLENCEK
        # write to hive test1 database cars_cleaned table in avro format
        input_df.write \
            .format("avro") \
            .mode("overwrite") \
            .option("compression", "bzip2") \
            .saveAsTable("default.cars_cleaned")

        return print("Successfully uploaded to Hive")

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
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        r = 6371  # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
        return c * r

    def format_dates_ML(self, input_df: DataFrame) -> DataFrame:
        df = input_df.withColumn("pickup_datetime",
                                   F.to_timestamp(F.col("pickup_datetime"), "yyyy-MM-dd HH🇲🇲ss")) \
            .withColumn("dropoff_datetime",
                        F.to_timestamp(F.col("dropoff_datetime"), "yyyy-MM-dd HH🇲🇲ss")) \
            .withColumn("pickup_year",
                        F.year(F.to_date(F.col("pickup_datetime")))) \
            .withColumn("pickup_month",
                        F.month(F.to_date(F.col("pickup_datetime")))) \
            .withColumn("pickup_dayofweek",
                        F.dayofweek(F.to_date(F.col("pickup_datetime")))) \
            .withColumn("pickup_hour",
                        F.hour(F.col("pickup_datetime"))) \
            .withColumn("pickupDayofWeek_TR",
                        MyHelpers.switch_tr_day(F.col("pickup_dayofweek"))) \
            .withColumn("pickupMonth_TR",
                        MyHelpers.switch_month_day(F.col("pickup_month"))) \
            .withColumn("haversine_distance(km)",
                        MyHelpers.haversine(F.col("pickup_longitude"), F.col("pickup_latitude"),
                                           F.col("dropoff_longitude"),
                                           F.col("dropoff_latitude")))
        return df

    def format_dates_DE(self, input_df: DataFrame) -> DataFrame:
        df = input_df.withColumn("pickup_datetime",
                                   F.to_timestamp(F.col("pickup_datetime"), "yyyy-MM-dd HH🇲🇲ss")) \
            .withColumn("dropoff_datetime",
                        F.to_timestamp(F.col("dropoff_datetime"), "yyyy-MM-dd HH🇲🇲ss")) \
            .withColumn("pickup_year",
                        F.year(F.to_date(F.col("pickup_datetime")))) \
            .withColumn("pickup_month",
                        F.month(F.to_date(F.col("pickup_datetime")))) \
            .withColumn("pickup_dayofweek",
                        F.dayofweek(F.to_date(F.col("pickup_datetime")))) \
            .withColumn("pickup_hour",
                        F.hour(F.col("pickup_datetime"))) \
            .withColumn("pickupDayofWeek_TR",
                        MyHelpers.switch_tr_day(F.col("pickup_dayofweek"))) \
            .withColumn("pickupMonth_TR",
                        MyHelpers.switch_month_day(F.col("pickup_month"))) \
            .withColumn("haversine_distance(km)",
                        MyHelpers.haversine(F.col("pickup_longitude"), F.col("pickup_latitude"),
                                           F.col("dropoff_longitude"),
                                           F.col("dropoff_latitude")))

            #TODO: ELASTİC İÇİN FEATURELAR EKLE

        return df