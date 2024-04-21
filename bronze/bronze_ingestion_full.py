# Databricks notebook source
# DBTITLE 1,Imports
import json
from pyspark.sql import types

# COMMAND ----------

# DBTITLE 1,Function declarations
def import_schema(table_name):
    with open(f'schemas/{table_name}.json', 'r') as open_file:
        schema = json.load(open_file)
    return types.StructType.fromJson(schema)

def table_exists(table_name):
    check_query = f"show tables in bronze_gc like '{table_name}'"
    df = spark.sql(check_query)
    return df.count() > 0

def get_raw_tables():
    fs_raw_tables = dbutils.fs.ls("/mnt/gc-bricks/raw/full-load")
    raw_tables = []
    for t in fs_raw_tables:
        raw_tables.append(t.name[:-1])
    return raw_tables

# COMMAND ----------

# DBTITLE 1,Full Load
for raw_table in get_raw_tables():

    if not table_exists(raw_table):
        print(f"Running initial full load for table {raw_table}...")
        raw_schema = import_schema(raw_table)
        raw_path = f"/mnt/gc-bricks/raw/full-load/{raw_table}"
        
        df_raw = (
            spark
            .read
            .schema(raw_schema)
            .csv(raw_path, header=True)
        )

        (df_raw
        .write
        .format('delta')
        .saveAsTable(f"bronze_gc.{raw_table}")
        )
        print(f"Full load for table {raw_table} complete")


# COMMAND ----------

# DBTITLE 1,CDC Incremental Load as Stream
# The only table we support incremental load is tb_lobby_stats_player

cdc_table_name = "tb_lobby_stats_player"

stream_schema = import_schema(cdc_table_name)
stream_schema = stream_schema.add('Op', data_type=types.StringType(), nullable=False, metadata={})
cdc_raw_path = f"/mnt/gc-bricks/raw/cdc/{cdc_table_name}"

df_cdc = (
    spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(stream_schema)
)

df_cdc

# COMMAND ----------

# DBTITLE 1,Sandbox
raw_table = "tb_lobby_stats_player"
raw_schema = import_schema(raw_table)
raw_path = f"/mnt/gc-bricks/raw/cdc/{raw_table}"

df_raw = (
    spark
    .read
    # .schema(raw_schema)
    .csv(raw_path, header=True)
)

df_raw.display()

# COMMAND ----------


