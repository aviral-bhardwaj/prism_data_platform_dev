# Databricks notebook source
dbutils.widgets.text('gidwave','')
GidWave = dbutils.widgets.get('gidwave')
dbutils.widgets.text('jobrunid','')
dbutils.widgets.text('username','')
dbutils.widgets.text('jobid','')
jr=dbutils.widgets.get('jobrunid')
un=dbutils.widgets.get('username')
jid=dbutils.widgets.get('jobid')

# COMMAND ----------

if GidWave == '':
  dbutils.notebook.exit('Please provide a value for GidWave')
else:
  result = ",".join(f"'{x}'" for x in GidWave.split(","))

  print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC `Importing Dependencies`

# COMMAND ----------

from pyspark.sql import functions as F
from linkage_table import *

# COMMAND ----------

# MAGIC %md
# MAGIC `Variables`

# COMMAND ----------

gold_schema = 'prism_gold.us_hotels'
silver_schema = 'prism_silver.canonical_tables'
target_table = 'tab_master'
instrument = 'us hotels'

# COMMAND ----------

# MAGIC %md
# MAGIC `Sources`

# COMMAND ----------

# DBTITLE 1,Source Tables
df_instrument = spark.table(f"""{silver_schema}.dim_instrument""").filter(F.lower(F.col("nam_instrument")) == instrument)
gid_instrument = df_instrument.select('gid_instrument').collect()[0]['gid_instrument']

df_wave = spark.table(f"""{silver_schema}.dim_wave""").filter(F.col("gid_instrument") == gid_instrument ).filter(F.expr(f"gid_wave IN ({result})"))
latest_wave_dte = df_wave.agg(F.max('wave_dte_start').alias('wave_dte_start')).collect()[0]['wave_dte_start']
gid_wave = df_wave.filter(F.col("wave_dte_start") == latest_wave_dte).select('gid_wave').collect()[0]['gid_wave']
_ = df_wave.filter(F.expr(f"gid_wave IN ({result})")).select('desc_wave').collect()[0]['desc_wave']


df_response = spark.table(f"""{gold_schema}.st_fact_response""")\
                   .filter(F.col("gid_instrument") == gid_instrument)\
                   .filter(F.expr(f"gid_wave IN ({result})"))\
                   .filter(F.col('deleted_in_source')=='false')\
                   .select("gid_response","gid_respondent","gid_question","gid_provider","gid_episode","gid_wave","txt_answer","num_answer")

df_question = spark.table(f"""{silver_schema}.dim_question""")\
                   .filter(F.col("gid_instrument") == gid_instrument)\
                   .select("gid_question","desc_question_grp","id_universal_question","id_survey_question","disp_question")


df_mod_provider = spark.table(f"""{gold_schema}.mod_provider""")\
                     .filter(F.expr(f"gid_wave IN ({result})"))\
                     .select("val_nps", "val_nps_category", "cde_nps_category", "nam_provider", "nam_provider_parent", "Last_stay_purpose", 
                            "Loyalty_Program", "Loyalty_program_membership", "Loyalty_program_tier", "CoBranded_Credit_Card", "CoBranded_credit_card_membership", "provider_segment","gid_respondent","gid_provider")
                     
df_mod_respondent = spark.table(f"""{gold_schema}.mod_respondent""")\
                     .filter(F.col("gid_instrument") == gid_instrument)\
                     .filter(F.expr(f"gid_wave IN ({result})"))\
                     .select("desc_wave", "age_range", "gender", "qualified_status", "sampling_phase", "termination_code", "income_range", "region", "state","zip_code","gid_respondent","id_respondent")


df_mod_episode = spark.table(f"""{gold_schema}.mod_episode""")\
                     .filter(F.expr(f"gid_wave IN ({result})"))\
                     .select("val_nps", "val_nps_category", "cde_nps_category", "nam_long_episode", "grp_episode", "episode_assigned","likelihood_to_recommend","gid_respondent","gid_provider","gid_episode")

df_mod_loyalty_attributes = spark.table(f"""{gold_schema}.mod_loyalty_attributes""")\
                     .filter(F.expr(f"gid_wave IN ({result})"))\
                     .select("gid_respondent","gid_question","gid_provider","Top 2","Top 1","Attribute","Attribute category","Loyalty program code")

df_mod_kpc_attributes = spark.table(f"""{gold_schema}.mod_kpc_attributes""")\
                     .filter(F.expr(f"gid_wave IN ({result})"))\
                     .select("gid_respondent","gid_question","gid_provider","Top 2","Top 1","Attribute","Attribute category")


df_mod_loyalty_program = spark.table(f"""{gold_schema}.mod_loyalty_program""")\
                     .filter(F.expr(f"gid_wave IN ({result})"))\
                     .withColumnRenamed("val_nps","val_nps_loyalty")\
                     .withColumnRenamed("val_nps_category","val_nps_category_loyalty")\
                     .withColumnRenamed("cde_nps_category","cde_nps_category_loyalty")\
                     .withColumnRenamed("nam_provider_parent","nam_provider_parent_r")\
                     .drop("gid_wave")

df_gold_mapping = spark.table(f"""{silver_schema}.dim_gold_mapping""")\
                     .filter(F.expr(f"gid_wave IN ({result})"))\
                     .filter(F.col("gid_instrument") == gid_instrument)\
                     .filter(F.col("map_field_value_1").contains("Q_LoyaltyProgram_Memberr")|F.col("map_field_value_1").contains("Q_LoyaltyProgram_Tierr") |F.col("map_field_value_1").contains("Q_CoBranded_Credit_Cardr"))\
                     .select("map_field_value_1","map_field_key_3","map_field_value_3","map_field_value_4")

df_gold_mapping_nam_provider = spark.table(f"""{silver_schema}.dim_gold_mapping""")\
                     .filter(F.expr(f"gid_wave IN ({result})"))\
                     .filter(F.col("gid_instrument") == gid_instrument)\
                     .filter((F.col("map_field_key_1") == ("Provider name")) & (F.col("map_field_key_2")==("nam_provider_parent")))\
                     .select("map_field_value_1","map_field_value_2")\
                     .withColumnRenamed("map_field_value_1","nam_provider")\
                     .withColumnRenamed("map_field_value_2","nam_provider_parent")

df_provider = spark.table(f"""{silver_schema}.dim_provider""")\
                   .filter(F.col("gid_instrument") == gid_instrument)\
                   .join(df_gold_mapping_nam_provider, on='nam_provider', how='left')\
                   .select("gid_provider","nam_provider", 'nam_provider_parent')\
                   .withColumnRenamed("nam_provider","nam_provider_r")\
                   .withColumnRenamed("nam_provider_parent","nam_provider_parent_r")
                   
              
df_mod_episode_deep_dive = spark.table(f"""{gold_schema}.mod_episode_deep_dive""")\
                   .filter(F.expr(f"gid_wave IN ({result})"))\
                   .drop("gid_wave","id_survey_question","dte_create")

# COMMAND ----------

# DBTITLE 1,stg_response
df_response = df_response.join(df_question,["gid_question"],"inner")

# COMMAND ----------

df_response = df_response.filter((F.col("desc_question_grp").isin(["Loyalty_Program_NPS_2", "Loyalty_Program_NPS_1","Loyalty Program Verbatim_1","Loyalty Program Verbatim_2","Loyalty Program Attributes","Loyalty Program Member","Loyalty Program Tier","Respondent total time to complete","Satisfied with the time taken","Episode deep dive","Hotel considered","Hotel Quoted","Hotel channel","Hotel reasons","Hotel Provider Last Stay","CoBranded Credit Card",])) | (F.col("id_universal_question").isin(["uid_Provider_NPS", "uid_KPC_rating", "uid_Episode_Screener","uid_Episode_NPS","uid_Episode_Recommended","uid_Episode_Ease","uid_Provider_NPS_Verbatim","uid_Episode_NPS_Verbatim"])))

# COMMAND ----------

# DBTITLE 1,Mod_respondent
df_master_respondent = df_response.join(df_mod_respondent,["gid_respondent"],"inner")

# COMMAND ----------

# DBTITLE 1,Mod_provider
df_master_provider = df_master_respondent.join(df_mod_provider,["gid_respondent","gid_provider"],"left")

# COMMAND ----------

# DBTITLE 1,Mod_episode
df_master_episode_false = df_master_provider.filter(~((F.col("gid_episode") == -1) &(F.col("desc_question_grp") != "uid_Episode_Screener")))
df_master_episode_true = df_master_provider.filter(((F.col("gid_episode") == -1) &(F.col("desc_question_grp") != "uid_Episode_Screener")))

df_master_episode = df_master_episode_false.alias("df_master_episode_false").join(df_mod_episode.alias("df_mod_episode"),["gid_provider","gid_episode","gid_respondent"],"inner").drop(df_master_episode_false.val_nps,df_master_episode_false.val_nps_category,df_master_episode_false.cde_nps_category)

df_master_episode = df_master_episode.unionByName(df_master_episode_true, allowMissingColumns=True)

# COMMAND ----------

# DBTITLE 1,mod_loyalty_attributes & mod_kpc_attributes
df_mod_attributes = df_mod_loyalty_attributes.unionByName(df_mod_kpc_attributes, allowMissingColumns=True)
df_master_attributes = df_master_episode.join(df_mod_attributes,["gid_respondent","gid_provider","gid_question"],"left")

# COMMAND ----------

# DBTITLE 1,loyalty_nps_table
df_master_loyalty_program = df_master_attributes.withColumn("Loyalty program code", F.when(F.col("id_survey_question").isin(["Q_Loyalty_Program_NPS_1","Q_Loyalty_Program_NPS_2","Q_Loyalty_Program_NPS_Verbatim_1","Q_Loyalty_Program_NPS_Verbatim_2","Q_LoyaltyProgram_1","Q_LoyaltyProgram_2"]), F.substring(F.col("id_survey_question"), -1, 1)).otherwise(F.col("Loyalty program code")))

df_master_loyalty_program = df_master_loyalty_program.join(df_mod_loyalty_program,["gid_respondent","Loyalty program code"],"left")

df_master_loyalty_program = df_master_loyalty_program\
                            .withColumn("val_nps", F.when(F.col("val_nps_loyalty").isNotNull(), F.col("val_nps_loyalty")).otherwise(F.col("val_nps")))\
                            .withColumn("val_nps_category", F.when(F.col("val_nps_category_loyalty").isNotNull(), F.col("val_nps_category_loyalty")).otherwise(F.col("val_nps_category")))\
                            .withColumn("cde_nps_category", F.when(F.col("cde_nps_category_loyalty").isNotNull(), F.col("cde_nps_category_loyalty")).otherwise(F.col("cde_nps_category"))).drop("val_nps_loyalty", "val_nps_category_loyalty", "cde_nps_category_loyalty")\
                            .withColumn("nam_provider_parent", F.when(F.col("nam_provider_parent").isNull(), F.col("nam_provider_parent_r")).otherwise(F.col("nam_provider_parent"))).drop("nam_provider_parent_r")\
                            .withColumn("Loyalty_program", F.when(F.col("Loyalty_program").isNull(), F.col("Loyalty program")).otherwise(F.col("Loyalty_program"))).drop("Loyalty program")\
                            .withColumn("Loyalty_program_membership", F.when(F.col("Loyalty_program_membership").isNull(), F.col("Loyalty program membership")).otherwise(F.col("Loyalty_program_membership"))).drop("Loyalty program membership")\
                            .withColumn("Loyalty_program_tier", F.when(F.col("Loyalty_program_tier").isNull(), F.col("Loyalty program tier")).otherwise(F.col("Loyalty_program_tier"))).drop("Loyalty program tier")


# COMMAND ----------

# DBTITLE 1,Loyalty Member, Tier and Co-branded Card Mapping for Parent Provider:
df_parent_provider = df_master_loyalty_program.alias("df_master_loyalty_program").join(df_gold_mapping.alias("df_gold_mapping"),df_master_loyalty_program["id_survey_question"] == df_gold_mapping["map_field_value_1"], "left")

df_parent_provider = df_parent_provider\
                            .withColumn("nam_provider_parent", F.when(F.col("map_field_value_1").isNotNull(), F.col("map_field_value_4")).otherwise(F.col("nam_provider_parent")))\
                            .withColumn("Loyalty_program", F.when((F.col("map_field_value_1").isNotNull()) & (F.col("map_field_key_3") == "Loyalty_Program"), F.col("map_field_value_3")).otherwise(F.col("Loyalty_program")))\
                           .drop("map_field_value_1", "map_field_key_3", "map_field_value_3","map_field_value_4")

# COMMAND ----------

# DBTITLE 1,Hotel Provider Stay Mapping
# nam_parent_provider is not mapped need to discuss this

df_parent_provider_false = df_parent_provider.filter(~(F.col("id_survey_question").contains("Q_HotelProvider_Last_Stay")))
df_parent_provider_true = df_parent_provider.filter((F.col("id_survey_question").contains("Q_HotelProvider_Last_Stay")))

df_hotel_provider = df_parent_provider_true.join(df_provider,["gid_provider"],"left")

df_hotel_provider = df_hotel_provider.withColumn("nam_provider", F.when(F.col("nam_provider_r").isNotNull(), F.col("nam_provider_r")).otherwise(F.col("nam_provider"))).drop("nam_provider_r")
df_hotel_provider = df_hotel_provider.withColumn("nam_provider_parent", F.when(F.col("nam_provider_parent_r").isNotNull(), F.col("nam_provider_parent_r")).otherwise(F.col("nam_provider_parent"))).drop("nam_provider_parent_r")


df_hotel_provider = df_hotel_provider.unionByName(df_parent_provider_false, allowMissingColumns=True)

# COMMAND ----------

# DBTITLE 1,Episode Deep Dive
df_episode_deep_dive = df_hotel_provider.join(df_mod_episode_deep_dive,["gid_respondent","gid_provider","gid_episode","gid_question","txt_answer","num_answer"],"left")

df_episode_deep_dive = df_episode_deep_dive\
                            .withColumn("disp_question", F.when(F.col("disp_question_long").isNotNull(), F.col("disp_question_long")).otherwise(F.col("disp_question")))\
                           .drop("disp_question_long")

# COMMAND ----------

# DBTITLE 1,Miscellaneous Transformation
df_master_final = df_episode_deep_dive\
                            .withColumn("disp_question",F.regexp_replace(F.col("disp_question"), "Your experience with Provider for ", ""))\
                            .withColumn("val_nps_category",F.col("val_nps_category")*100)\
                            .withColumn("dte_create",F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC `final selection`

# COMMAND ----------

# DBTITLE 1,Columns selection
df_master_final = df_master_final.select(
    "gid_respondent",
    "gid_wave",
    "gid_question",
    "gid_provider",
    "gid_episode",
    "id_respondent",
    "id_survey_question",
    "txt_answer",
    "num_answer",
    "disp_question",
    "desc_question_grp",
    "desc_wave",
    "nam_provider",
    "nam_provider_parent",
    "nam_long_episode",
    "grp_episode",
    "val_nps",
    "val_nps_category",
    "cde_nps_category",
    "episode_assigned",
    "provider_segment",
    "Last_stay_purpose",
    "Loyalty_program",
    "Loyalty_program_membership",
    "Loyalty_program_tier",
    "CoBranded_Credit_Card",
    "CoBranded_credit_card_membership",
    "likelihood_to_recommend",
    "Top 2",
    "Top 1",
    "Attribute",
    "Attribute category",
    "age_range",
    "gender",
    "qualified_status",
    "sampling_phase",
    "termination_code",
    "income_range",
    "region",
    "state",
    "zip_code",
    "Episode answer text sorting",
    "dte_create")

# COMMAND ----------

# DBTITLE 1,Space removal from columns
df_master_final = df_master_final.toDF(*[c.replace(" ", "_") for c in df_master_final.columns])

# COMMAND ----------

# DBTITLE 1,DDL for table
# %sql
# CREATE TABLE prism_gold.us_hotels.tab_master (
#     gid_respondent INT,
#     gid_wave INT,
#     gid_question INT,
#     gid_provider INT,
#     gid_episode INT,
#     id_respondent STRING,
#     id_survey_question STRING,
#     txt_answer STRING,
#     num_answer STRING,
#     disp_question STRING,
#     desc_question_grp STRING,
#     desc_wave STRING,
#     nam_provider STRING,
#     nam_provider_parent STRING,
#     nam_long_episode STRING,
#     grp_episode STRING,
#     val_nps INT,
#     val_nps_category INT,
#     cde_nps_category STRING,
#     episode_assigned INT,
#     provider_segment STRING,
#     last_stay_purpose STRING,
#     loyalty_program STRING,
#     loyalty_program_membership STRING,
#     loyalty_program_tier STRING,
#     cobranded_credit_card STRING,
#     cobranded_credit_card_membership STRING,
#     likelihood_to_recommend STRING,
#     top_2 STRING,
#     top_1 STRING,
#     attribute STRING,
#     attribute_category STRING,
#     age_range STRING,
#     gender STRING,
#     qualified_status STRING,
#     sampling_phase STRING,
#     termination_code STRING,
#     income_range STRING,
#     region STRING,
#     state STRING,
#     zip_code STRING,
#     episode_answer_text_sorting STRING,
#     dte_create TIMESTAMP
# );

# COMMAND ----------

# MAGIC %md
# MAGIC `delete old enteries and ingest new records`

# COMMAND ----------

spark.sql(f"""DELETE FROM prism_gold.us_hotels.tab_master WHERE gid_wave = '{gid_wave}' """)

# COMMAND ----------

# MAGIC %md
# MAGIC Data Ingestion

# COMMAND ----------

# DBTITLE 1,Data Ingestion
df_master_final.select(spark.table(f"""{gold_schema}.{target_table}""").columns).write.mode('append').saveAsTable(f"""{gold_schema}.{target_table}""")

# COMMAND ----------

target_table=f"{gold_schema}.{target_table}"

insert_data(
  spark=spark,
  table_name="prism_bronze.data_plat_control.job_run_details",
  target_table_name=target_table,
  notebook_name=dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[-1],
  jobrunid=jr,
  jobid=jid,
  username=un
)