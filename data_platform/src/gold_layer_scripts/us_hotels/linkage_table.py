from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

def insert_data(spark, table_name, target_table_name, notebook_name, jobrunid,jobid, username):
    schema = StructType([
        StructField("target_table_name", StringType(), True),
        StructField("notebook_name", StringType(), True),
        StructField("jobrunid", StringType(), True),
        StructField("jobid", StringType(), True),
        StructField("username", StringType(), True),
    ])

    data = [(target_table_name, notebook_name, jobrunid,jobid, username)]
    df = spark.createDataFrame(data, schema)

    # Add timestamp in Spark (correct type)
    df = df.withColumn("dte_update", F.current_timestamp())

    df.write.mode("append").saveAsTable(table_name)
