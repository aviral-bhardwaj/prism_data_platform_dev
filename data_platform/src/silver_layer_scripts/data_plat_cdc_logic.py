from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window as W

from pyspark.sql import SparkSession
from functools import reduce
from delta.tables import DeltaTable

import warnings

def create_index_column(
    df,
    index_col_name,
    start_value = 1,
):
    """Create an monotonically increasing index column, starting with 1

    Args:
        df (DataFrame): DataFrame in which the index column should be inserted
        index_col_name (str): Name of the newly created index column
        start_value (int): initial value to start increasing. Defaults to 1

    Returns:
        DataFrame: DataFrame with new index column
    """

    start_value = int(start_value)

    start_new = start_value - 1

    df = df.withColumn("idx", F.monotonically_increasing_id())
    windowSpec = W.orderBy("idx")
    df = df.withColumn(index_col_name, F.row_number().over(windowSpec))
    df = df.withColumn(index_col_name, F.col(index_col_name) + start_new)
    df = df.drop("idx")

    return df

def mark_empty_null_to_empty_string(df):
    for col_name in df.columns:
        df = df.withColumn(col_name, F.when(F.col(col_name)=='', F.lit(None)).otherwise(F.col(col_name)))
    return df

def dataplatform_cdc(tables_dict,key_cols=None,exclusion_cols=None):
    spark = SparkSession.builder.appName("data_plat").getOrCreate()
    silver_layer = "prism_silver.canonical_tables"
    warnings.filterwarnings("ignore", message=".*No Partition Defined for Window operation.*")

    if key_cols is None:
        raise ValueError("key_cols cannot be None")

    if exclusion_cols is None:
        exclusion_cols = ['gid', 'dte_create', 'dte_update', 'ind_current']
    
    dict_dfs = tables_dict
    
    current_exclusion_cols = exclusion_cols.copy()

    for key, value in dict_dfs.items():

        print("SCD begins for :",key)
        df = value
        df_src = mark_empty_null_to_empty_string(df)

        cols = [col for col in df_src.columns if not any(substring in col for substring in current_exclusion_cols)]

        print("Hashvalue creation for src table begins")
        hash_cols_src = [F.col(col) for col in cols if col not in key_cols[key]]


        df_src = df_src.withColumn("hashvalue", F.sha2(F.concat_ws(" ", *hash_cols_src), 256))

        print("Hashvalue creation for src table completed")
        print("")

        print("Hashvalue creation for tgt table begins")

        #hash_cols_tgt = [F.col(col) for col in cols if col not in key_cols[key]]
    
        df_tgt = spark.sql(f"""SELECT * FROM {silver_layer}.{key} """)
        df_tgt = df_tgt.withColumn("hashvalue", F.sha2(F.concat_ws(" ", *hash_cols_src), 256))

        print("Hashvalue creation for tgt table completed")
        print("")
        
        print("Fetching max gid")
        
        gid_col = key.replace('dim','gid')
        

        if df_tgt.withColumn(gid_col,F.col(gid_col).cast(T.IntegerType())).select(F.max(gid_col)).collect()[0][0] is not None:
            max_gid = df_tgt.withColumn(gid_col,F.col(gid_col).cast(T.IntegerType())).select(F.max(gid_col)).collect()[0][0] +1
        else:
            max_gid = 1

        print("Fetched max gid")
        print("")

        join_condition = reduce(lambda a, b: a & b, [F.coalesce(df_src[col],F.lit(-9999)) == F.coalesce(df_tgt[col],F.lit(-9999)) for col in key_cols[key]])
        

        df_updated = df_src.join(df_tgt, join_condition & (df_src["hashvalue"] != df_tgt["hashvalue"]), 'inner').select(df_src["*"] , F.current_timestamp().alias('dte_updated'))


        print("Checking updated records for :",key)

        if df_updated.isEmpty():
            print("No updated records found for :",key)
            print("")
        else :
            print("Updated records found in :",key)

            print("Merge for table started :",f"{silver_layer}.{key}")

            delta_table = DeltaTable.forName(spark, f"{silver_layer}.{key}")
            update_expr = {col: f"df_src_updt.{col}" for col in df_updated.columns if col not in key_cols[key] + ['hashvalue','ind_current_record','dte_updated']}
            update_expr["dte_update"] = "df_src_updt.dte_updated"
            join_condition_merge = " AND ".join([f"coalesce(df_tgt_tbl.{col_name},-9999) = coalesce(df_src_updt.{col_name},-9999)" for col_name in key_cols[key]])


            delta_table.alias("df_tgt_tbl").merge(
                    df_updated.alias("df_src_updt"),
                    join_condition_merge
                ).whenMatchedUpdate(set=update_expr).execute()

            print("Merge completed for:",f"{silver_layer}.{key}")
            print("")

        
        print("Checking new records for :",key)

        df_new_records = df_src.join(df_tgt, join_condition , 'left_anti').select(df_src["*"])

        df_new_records = create_index_column(df_new_records , gid_col , max_gid)

        if df_new_records.isEmpty():
            print("No new records found for :",key)
            return df_new_records
                             
        else:
            print("New records found for :",key)
            df_new_records = df_new_records.withColumn("dte_create", F.current_timestamp()) \
                                           .withColumn("dte_update", F.current_timestamp()) 
            return   df_new_records
