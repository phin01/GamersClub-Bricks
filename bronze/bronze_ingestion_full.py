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

