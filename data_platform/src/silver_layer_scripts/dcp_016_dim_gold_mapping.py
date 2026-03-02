# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import sha2, concat_ws, col, coalesce, lit
from pyspark.sql.functions import col, lit, row_number, max as spark_max
from pyspark.sql.window import Window
from pyspark.sql.functions import min, max

# COMMAND ----------


# Creating Variables
silver_layer = "prism_silver"
bronze_layer = "prism_bronze"

canonical_tables_schema = "canonical_tables"
decipher_schema = "decipher"

gold_mapping_table = "gold_mapping_test"
dim_wave_table = "dim_wave"
dim_gold_mapping_table = "dim_gold_mapping"

instrument='dim_instrument'
wave = 'dim_wave'

# COMMAND ----------

df = (
    spark.read
         .table(f"{bronze_layer}.{decipher_schema}.{gold_mapping_table}")
)


# COMMAND ----------

df_wave = (
    spark.read
         .table(f"{silver_layer}.{canonical_tables_schema}.{wave}")
).select('gid_wave','layout_id','survey_id','gid_instrument')

# COMMAND ----------

# DBTITLE 1,Cell 9
df1=df.join(df_wave, on=["survey_id", "layout_id"], how='left')

# COMMAND ----------

# DBTITLE 1,Cell 9
df2 = df1

# COMMAND ----------

# Reading the existing data on the silver layer 
silver_df = (
    spark.read
         .table(f"{silver_layer}.{canonical_tables_schema}.{dim_gold_mapping_table}")
)

# COMMAND ----------

# DBTITLE 1,Untitled


# Helper: dynamically determine hash columns

def get_dynamic_hash_columns(df1, df2):
    common_cols = set(df1.columns).intersection(df2.columns)

    hash_cols = [
        c for c in common_cols
        if not (
            c.lower() == "hash_key"              # never hash the hash
            or c.lower().startswith("_")         # __START_AT, __END_AT
            or c.lower().startswith("dte_")      # dte_create, dte_update
            or c.lower().endswith("_at")          # created_at, updated_at
            or c.lower().endswith("_ts")          # timestamps
            or "file" in c.lower()                # file_name, file_path
        )
    ]

    return sorted(hash_cols)


# Get deterministic hash columns
hash_cols = get_dynamic_hash_columns(df2, silver_df)

print("Hash columns used:")
for c in hash_cols:
    print(c)


# -------------------------------
# Create hash_key for df2
# -------------------------------
df2 = df2.withColumn(
    "hash_key",
    sha2(
        concat_ws(
            "||",
            *[coalesce(col(c), lit("NULL")) for c in hash_cols]
        ),
        256
    )
)


# Create hash_key for silver_df
silver_df = silver_df.withColumn(
    "hash_key",
    sha2(
        concat_ws(
            "||",
            *[coalesce(col(c), lit("NULL")) for c in hash_cols]
        ),
        256
    )
)



# COMMAND ----------

# DBTITLE 1,Cell 13
#filtering new data to be pushed 
new_records_df = df2.join(
    silver_df,on="hash_key" ,
    how="left_anti"
)

# COMMAND ----------

# max key from Silver
max_gid = (
    silver_df
    .select(spark_max(col("gid_gold_mapping")).alias("max_gid"))
    .collect()[0]["max_gid"]
)
max_gid = max_gid if max_gid is not None else 0
# incremental key to new rows
w = Window.orderBy(lit(1))  # deterministic but arbitrary order


# COMMAND ----------

new_records_df = new_records_df.select(
    "mapping_category",
    "map_field_key_1", "map_field_value_1",
    "map_field_key_2", "map_field_value_2",
    "map_field_key_3", "map_field_value_3",
    "map_field_key_4", "map_field_value_4",
    "map_field_key_5", "map_field_value_5",
    "map_field_key_6", "map_field_value_6",
    "map_field_key_7", "map_field_value_7",
    "map_field_key_8", "map_field_value_8",
    "map_field_key_9", "map_field_value_9",
    "map_field_key_10", "map_field_value_10",
    "map_field_key_11", "map_field_value_11",
    "map_field_key_12", "map_field_value_12",
    "map_field_key_13", "map_field_value_13",
    "map_field_key_14", "map_field_value_14",
    "map_field_key_15", "map_field_value_15",
    "gid_wave",
    "gid_instrument"
)


# COMMAND ----------

print(new_records_df.count())
new_records_df_1=new_records_df.dropDuplicates()
print(new_records_df_1.count())

# COMMAND ----------


new_df = new_records_df_1.withColumn(
    "gid_gold_mapping",
    row_number().over(w) + lit(max_gid)
)

# COMMAND ----------

new_df.agg(
    min("gid_gold_mapping").alias("min_gid"),
    max("gid_gold_mapping").alias("max_gid")
).display()


# COMMAND ----------

df_final_2 = new_df.select(
    "mapping_category",
    "map_field_key_1", "map_field_value_1",
    "map_field_key_2", "map_field_value_2",
    "map_field_key_3", "map_field_value_3",
    "map_field_key_4", "map_field_value_4",
    "map_field_key_5", "map_field_value_5",
    "map_field_key_6", "map_field_value_6",
    "map_field_key_7", "map_field_value_7",
    "map_field_key_8", "map_field_value_8",
    "map_field_key_9", "map_field_value_9",
    "map_field_key_10", "map_field_value_10",
    "map_field_key_11", "map_field_value_11",
    "map_field_key_12", "map_field_value_12",
    "map_field_key_13", "map_field_value_13",
    "map_field_key_14", "map_field_value_14",
    "map_field_key_15", "map_field_value_15",
    "gid_wave",
    "gid_instrument",
    "gid_gold_mapping"


)


# COMMAND ----------

df_final_2 = (
    df_final_2.withColumn("dte_create", current_timestamp())
      .withColumn("dte_update", current_timestamp())
)

# COMMAND ----------

df_final_2.display()

# COMMAND ----------

df_final_2.write \
  .format("delta") \
  .mode("append") \
  .saveAsTable(f"{silver_layer}.{canonical_tables_schema}.{dim_gold_mapping_table}")
