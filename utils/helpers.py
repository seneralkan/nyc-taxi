from pyspark.sql import SparkSession, functions as F
from pyspark.sql import DataFrame


class MyHelpers:

    def get_spark_session(self, session_params: dict) -> SparkSession:
        ## Put your code here.
        spark = (
            SparkSession.builder
                .appName(session_params["appName"])
                .enableHiveSupport()
                .master(session_params["master"])
                .config("spark.executor.memory", session_params["executor_memory"])
                .config("spark.executor.instances", session_params["executor_instances"])
                .config("spark.executor.cores", session_params["executor_cores"])
                .getOrCreate()
        )
        return spark

    def get_data(self, spark_session: SparkSession, read_params: dict) -> DataFrame:
        ## Put your code here
        df = (spark_session.read.format(read_params["format"])
              .option("header", read_params["header"])
              .option("sep", read_params["sep"])
              .option("inferSchema", read_params["inferSchema"])
              .load(read_params["path"])
              )
              
        return df

    def format_dates(self, date_cols: list, input_df: DataFrame) -> DataFrame:
        ## Make timestamp of string dates/ts
        df = input_df.withColumn(date_cols[0] + "_new", 
                        F.to_timestamp(F.col(date_cols[0]), 'yyyy-MM-dd HH:mm:ss.SSSSSSx')) \
                    .withColumn(date_cols[1] + "_new", 
                        F.to_timestamp(F.col(date_cols[1]), 'yyyy-MM-dd HH:mm:ss.SSSSSx'))
        return df

    def make_nulls_to_unknown(self, input_df: DataFrame) -> DataFrame:
        ## Replace Null/None/Empty values with Unknown in string columns
        str_cols = [col[0] for col in input_df.dtypes if col[1] == "string"]
        df = input_df
        for my_col in str_cols:
            df = df.withColumn(my_col, F.when(
                (F.col(my_col).isNull() | (F.col(my_col) == "None")),
                "Unknown").otherwise(F.col(my_col)))

        return df

    def trim_str_cols(self, input_df) -> DataFrame:
        ## trim all string cols
        df = input_df
        str_col = [col for col in df.columns if col.startswith('string')]

        for i in range(len(str_col)):
            df.withColumn(str_col[i],F.trim(F.col(str_col[i])))
        
        return df

    def write_to_hive(self, input_df: DataFrame):
        # write to hive test1 database cars_cleaned table in avro format
        input_df.write \
            .format("avro") \
            .mode("overwrite") \
            .option("compression", "bzip2") \
            .saveAsTable("default.cars_cleaned")

        return print("Successfully uploaded to Hive")
