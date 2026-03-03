# Databricks notebook source
from pyspark.sql import functions as F
import os
from itertools import product

# COMMAND ----------

# For running on serverless
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

base_path = '/Volumes/prism_bronze/landing_volumes/landing_files/sources/decipher/'

# path for questions_mapping, survey
files = 'survey_files_endpoint_csv/'

# path for survey metadata
survey_metadata = 'survey_metadata_endpoint/'

# path for survey layouts
survey_layouts = 'survey_layouts_endpoint/'

# path to all surveys
all_surveys = 'all_surveys_endpoint/'

list_of_paths = [
    'survey_files_endpoint_csv/',
    'survey_metadata_endpoint/',
    'survey_layouts_endpoint/',
    'all_surveys_endpoint/'
]

# layouts combination
list_of_surveys = [
    '251006_26665',
    '251039_26403',
    '251024_26402',
    '251046_26401',
    '250810_26368'
]

list_of_file_names = [
    'questions_mapping',
    'answers_mapping',
    'variable_mapping',
    'survey_layouts',
    'survey_metadata',
    'all_surveys'
]

final_list = [f"{file}_{survey}" for file, survey in product(list_of_file_names, list_of_surveys)]

# COMMAND ----------

def eq_null_safe_join(
    df_base, 
    df_to_join, 
    list_of_cols: list, 
    join_type: str,
    drop_cols: str = True
    ):

    """Performs a join between two DataFrames with the EqNullSafe condition satisfied

    Args:
        df_base: Base DataFrame to be joined.
        df_to_join: DataFrame to be joined.
        list_of_cols: list of columns in which the join should be performed
        join_type: Type of Join
        drop_cols: Determines if cols from the right dataframe are dropped. Defaults to True

    Returns: Joined DataFrame
    """

    from functools import reduce

    #setting columns to compare
    join_cols = list_of_cols
    if list_of_cols is None:
        join_cols = df_base.columns

    join_condition = reduce(lambda x, y: x & y, [df_base[k].eqNullSafe(df_to_join[k]) for k in join_cols])

    drop_condition = [df_to_join[k] for k in join_cols]

    if drop_cols == True:

        df_final = df_base.join(df_to_join, on=join_condition, how=join_type).drop(*drop_condition)

    else:

        df_final = df_base.join(df_to_join, on=join_condition, how=join_type)
        
    return df_final


# COMMAND ----------

# Getting latest file
list_of_latest_files = []

if os.path.exists(base_path):
    for file_name in final_list:
        
        if 'metadata' in file_name:
            latest_file = max([base_path+survey_metadata+item for item in os.listdir(base_path+survey_metadata) if file_name in item.lower()])
        elif 'layouts' in file_name:
            latest_file = max([base_path+survey_layouts+item for item in os.listdir(base_path+survey_layouts) if '_'.join(file_name.split('_'))[:-6] in item.lower()])
        elif 'all_surveys' in file_name:
            latest_file = max([base_path+all_surveys+item for item in os.listdir(base_path+all_surveys) if '_'.join(file_name.split('_'))[:-13] in item.lower()])
        else:
            latest_file = max([base_path+files+item for item in os.listdir(base_path+files) if file_name in item.lower()])

        list_of_latest_files.append(latest_file)

list_of_latest_files = sorted(list(set(list_of_latest_files)))
list_of_latest_files

# COMMAND ----------

for item in list_of_latest_files[:]:

    if 'layouts' in item:

        survey_id = item.split('survey_layouts_endpoint/survey_layouts_')[-1].split('_202')[0].split('_')[0]
        table_type = item.split('survey_layouts_endpoint/')[-1].split('_25')[0].lower()
        table_name = item.split('/survey_layouts_endpoint/')[-1].split('_202')[0].lower()

        df_table = spark.sql(f"select * from prism_bronze.decipher.{table_type} where `__END_AT` is null and survey_id = {survey_id}")
    
    elif 'metadata' in item:

        survey_id = item.split('survey_metadata_')[-1].split('_202')[0].split('_')[0]
        layout_id = item.split('survey_metadata_2')[-1].split('_202')[0].split('_')[-1]
        table_type = item.split('survey_metadata_endpoint/')[-1].split('_25')[0].lower()
        table_name = item.split('survey_metadata_endpoint/')[-1].split('_202')[0].lower()

        df_table = spark.sql(f"select * from prism_bronze.decipher.{table_type} where `__END_AT` is null and survey_id = {survey_id} and layout_id = {layout_id}")


    elif 'all_surveys' in item:

        table_type = item.split('all_surveys_endpoint/')[-1].split('_202')[0].lower()
        table_name = item.split('all_surveys_endpoint/')[-1].split('_202')[0].lower()

        df_table = spark.sql(f"select * from prism_bronze.decipher.{table_type} where `__END_AT` is null")

    else:
            
        survey_id = item.split('mapping_')[-1].split('_202')[0].split('_')[0]
        layout_id = item.split('mapping_')[-1].split('_202')[0].split('_')[-1]
        table_type = item.split('survey_files_endpoint_csv/survey_file_')[-1].split('_25')[0].lower()
        table_name = item.split('survey_files_endpoint_csv/survey_file_')[-1].split('_202')[0].lower()

        df_table = spark.sql(f"select * from prism_bronze.decipher.{table_type} where `__END_AT` is null and survey_id = {survey_id} and layout_id = {layout_id}")
    
    print(f"Processing table {table_name}")
    

    if item.split('.')[-1] == 'csv':
        df_file = spark.read.option("quote", "\"").option("escape", "\"").option("delimiter", ",").csv(item, header=True, inferSchema=True)
    else:
        df_file = spark.read.option('multiLine', 'true').json(item)

        if 'metadata' in table_name:

            df_file = df_file.select(F.explode(F.col("variables")).alias("var")).select("var.*") 

    df_table = df_table.select(df_file.columns)

    print(f"Differences between tables. No difference for both means the latest file is exactly like the table that was processed and is in decipher.")
    
    cols_to_compare = df_file.columns
    if 'row_number' in cols_to_compare:
        cols_to_compare.remove('row_number')

    if eq_null_safe_join(df_table, df_file, cols_to_compare, 'leftanti', drop_cols=True).count() == 0 and eq_null_safe_join(df_file, df_table, cols_to_compare, 'leftanti', drop_cols=True).count() == 0:

        print('No difference found on the files.')
    
    else:

        print('Records that exists in the table, but not in the file:')   
        eq_null_safe_join(df_table, df_file, cols_to_compare, 'leftanti', drop_cols=True).display()

        print('Records that exists in the file, but not in the table:')   
        eq_null_safe_join(df_file, df_table, cols_to_compare, 'leftanti', drop_cols=True).display()

    print("="*80)
    print("\n")


