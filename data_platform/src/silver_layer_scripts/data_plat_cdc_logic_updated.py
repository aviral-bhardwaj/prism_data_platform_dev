from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window as W
from pyspark.sql import SparkSession
from functools import reduce
from delta.tables import DeltaTable
import warnings
from pyspark.sql.window import Window

def _infer_gid_col(table_name: str, target_cols: list[str]) -> str:
    """
    Infer gid column from dim table name.
    Examples:
      dim_product        -> gid_product
      dim_product_temp   -> gid_product   (strip _temp)
    Falls back to the only gid_* column in target if inference doesn't match.
    """
    base = table_name
    if base.startswith("dim_"):
        base = base[len("dim_"):]
    if base.endswith("_temp"):
        base = base[:-len("_temp")]
    if base.endswith("_test"):
        base = base[:-len("_test")]
    gid_col = f"gid_{base}"

    if gid_col in target_cols:
        return gid_col

    # Fallback: if table has exactly one gid_* column, use it
    gid_candidates = [c for c in target_cols if c.startswith("gid_")]
    if len(gid_candidates) == 1:
        return gid_candidates[0]

    raise ValueError(
        f"Could not infer gid column for table '{table_name}'. "
        f"Tried '{gid_col}'. Target gid_* candidates: {gid_candidates}"
    )

def mark_empty_null_to_empty_string(df):
    for col_name in df.columns:
        df = df.withColumn(col_name, F.when(F.col(col_name) == '', F.lit(None)).otherwise(F.col(col_name)))
    return df

def create_index_column(df, index_col_name, start_value=1):
    start_value = int(start_value)
    start_new = start_value - 1

    df = df.withColumn("_idx_tmp", F.monotonically_increasing_id())
    windowSpec = W.orderBy("_idx_tmp")
    df = df.withColumn(index_col_name, F.row_number().over(windowSpec))
    df = df.withColumn(index_col_name, F.col(index_col_name) + F.lit(start_new))
    df = df.drop("_idx_tmp")
    return df

def _get_historical_gids(spark, table_name, silver_layer="prism_silver.canonical_tables", key_cols=None, gid_col=None):

    # Creating list of older versions   
    list_of_previous_versions = [item[0] for item in spark.sql(f"describe history {silver_layer}.{table_name}").filter(~F.col('operation').isin(['DELETE', 'OPTIMIZE'])).select('version').collect()]

    # Getting second to last version if there are just two versions
    second_to_last_version = list_of_previous_versions[0]

    # Getting dataframe
    df = (
        spark.read.format("delta").option("versionAsOf", second_to_last_version).table(f"{silver_layer}.{table_name}")
        .select([gid_col] + key_cols[table_name])
        )
    
    return df        


def dataplatform_cdc(tables_dict, key_cols=None, exclusion_cols=None, silver_layer="prism_silver.canonical_tables", anchored_version=None):
    spark = SparkSession.builder.appName("data_plat").getOrCreate()
    warnings.filterwarnings("ignore", message=".*No Partition Defined for Window operation.*")

    if key_cols is None:
        raise ValueError("key_cols cannot be None")

    if exclusion_cols is None:
        # 'gid' as substring will exclude gid_* columns as well
        exclusion_cols = ['gid', 'dte_create', 'dte_update', 'ind_current']

    for table_name, df in tables_dict.items():
        print("SCD begins for :", table_name)

        df_src = mark_empty_null_to_empty_string(df)

        # Read target
        full_table_name = f"{silver_layer}.{table_name}"
        df_tgt = spark.table(full_table_name)

        # Resolve gid column robustly (handles _temp naming)
        gid_col = _infer_gid_col(table_name, df_tgt.columns)

        # Build list of non-excluded cols from SOURCE
        src_cols = [
            c for c in df_src.columns
            if not any(excl in c for excl in exclusion_cols)
        ]

        # Ensure target has any new source columns (for hashing consistency)
        for c in src_cols + key_cols[table_name]:
            if c not in df_tgt.columns:
                df_tgt = df_tgt.withColumn(c, F.lit(None).cast(df_src.schema[c].dataType))

        # Hash columns = all comparable cols except keys
        hash_cols = [c for c in src_cols if c not in key_cols[table_name]]

        print("Hashvalue creation for src table begins")
        df_src = df_src.withColumn(
            "hashvalue",
            F.sha2(F.concat_ws(" ", *[F.col(c).cast("string") for c in hash_cols]), 256)
        )
        print("Hashvalue creation for src table completed\n")

        print("Hashvalue creation for tgt table begins")
        df_tgt = df_tgt.withColumn(
            "hashvalue",
            F.sha2(F.concat_ws(" ", *[F.col(c).cast("string") for c in hash_cols]), 256)
        )
        print("Hashvalue creation for tgt table completed\n")

        # print("Fetching max gid")
        # max_gid_row = df_tgt.select(F.max(F.col(gid_col).cast("long")).alias("mx")).collect()[0]["mx"]
        # max_gid = (max_gid_row + 1) if max_gid_row is not None else 1
        # print("Fetched max gid\n")

        # Null-safe join condition on keys (no -9999 hacks, no datatype issues)
        join_condition = reduce(
            lambda a, b: a & b,
            [df_src[k].eqNullSafe(df_tgt[k]) for k in key_cols[table_name]]
        )

        # UPDATED records = key match but hash differs
        df_updated = (
            df_src.join(df_tgt, join_condition & (df_src["hashvalue"] != df_tgt["hashvalue"]), "inner")
                  .select(df_src["*"], F.current_timestamp().alias("dte_updated"))
        )

        print("Checking updated records for :", table_name)
        if df_updated.isEmpty():
            print("No updated records found for :", table_name, "\n")
        else:
            print("Updated records found in :", table_name)
            print("Merge for table started :", full_table_name)

            delta_table = DeltaTable.forName(spark, full_table_name)

            # Update all src columns except keys + helper cols
            skip_cols = set(key_cols[table_name] + ["hashvalue", "dte_updated"])
            update_expr = {c: f"df_src_updt.{c}" for c in df_updated.columns if c not in skip_cols}
            update_expr["dte_update"] = "df_src_updt.dte_updated"

            merge_condition = " AND ".join([f"df_tgt_tbl.{k} <=> df_src_updt.{k}" for k in key_cols[table_name]])

            (delta_table.alias("df_tgt_tbl")
                       .merge(df_updated.alias("df_src_updt"), merge_condition)
                       .whenMatchedUpdate(set=update_expr)
                       .execute())

            print("Merge completed for:", full_table_name, "\n")

        # NEW records = not present in target by key
        print("Checking new records for :", table_name)
        df_new_records = df_src.join(df_tgt, join_condition, "left_anti").select(df_src["*"])

        # Add gid_* values

        # initially, check if GID previously existed
        df_old_gids = _get_historical_gids(spark, table_name, silver_layer, key_cols, gid_col)

        # Build a fresh join condition between df_new_records and df_old_gids
        historical_join_condition = reduce(
            lambda a, b: a & b,
            [df_new_records[k].eqNullSafe(df_old_gids[k]) for k in key_cols[table_name]]
        )

        # joining to new_records
        df_new_records_join = df_new_records.join(df_old_gids, historical_join_condition, 'left')

        # Drop the duplicate key cols that came from df_old_gids
        for k in key_cols[table_name]:
            df_new_records_join = df_new_records_join.drop(df_old_gids[k])
            

        # joining to new_records
        # df_new_records_join = df_new_records.join(df_old_gids, join_condition, 'left')

        # split df in two
        df_new_records_with_gids = df_new_records_join.where(~F.col(gid_col).isNull())

        df_new_records_no_gids = df_new_records_join.where(F.col(gid_col).isNull())
       
        # Getting universally max gid

        max_from_tgt = df_tgt.select(F.max(F.col(gid_col).cast("long"))).collect()[0][0] or 0
        max_from_history = df_old_gids.select(F.max(F.col(gid_col).cast("long"))).collect()[0][0] or 0
        max_universal_gid = max(max_from_tgt, max_from_history) + 1
        # max_universal_gid = df_tgt.select(F.max(gid_col)).collect()[0][0] + 1
        
        # Assigning new gids for previously inexisting records
        df_new_records_no_gids_final = create_index_column(df_new_records_no_gids, gid_col, max_universal_gid)

        # Creating the final dataframe
        df_new_records_final = df_new_records_with_gids.unionByName(df_new_records_no_gids_final)


        if df_new_records_final.isEmpty():
            print("No new records found for :", table_name)
            return df_new_records_final
        else:
            print("New records found for :", table_name)
            df_new_records_final = (
                df_new_records_final
                .withColumn("dte_create", F.current_timestamp())
                .withColumn("dte_update", F.current_timestamp())
            )
            return df_new_records_final
