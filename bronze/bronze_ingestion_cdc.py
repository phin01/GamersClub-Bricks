# Databricks notebook source
# upsert delta concept based on repo
# https://github.com/anneglienke/101_upsert-delta

# COMMAND ----------

# DBTITLE 1,Imports
import json
from pyspark.sql import types
from pyspark.sql import functions
from pyspark.sql import window
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Function Declarations
def import_schema(table_name):
    with open(f'schemas/{table_name}.json', 'r') as open_file:
        schema = json.load(open_file)
    return types.StructType.fromJson(schema)

# COMMAND ----------

# DBTITLE 1,Variable setup
# The only table we currently support incremental load is tb_lobby_stats_player
cdc_table_name = "tb_lobby_stats_player"

# The schema for CDC tables should be the same as full load, with an additional 'Op' column to categorize the operation
# 'D' for Delete, 'U' for Update, 'I' for Insert
stream_schema = import_schema(cdc_table_name)
stream_schema = stream_schema.add('Op', data_type=types.StringType(), nullable=False, metadata={})

cdc_raw_path = f"/mnt/gc-bricks/raw/cdc/{cdc_table_name}"
delta_table = DeltaTable.forName(spark, f"bronze_gc.{cdc_table_name}")

# COMMAND ----------

# get all CDC files
# once we define a frequency for this execution, we could filter the load to avoid checking older files
df_cdc_files = (
            spark
            .read
            .schema(stream_schema)
            .csv(cdc_raw_path, header=True)
        )

# COMMAND ----------

# Create a window function to get only the latest entry based on a composite key, idLobbyPlayer + idPlayer
w = window.Window.partitionBy(["idLobbyGame", "idPlayer"]).orderBy(functions.desc("dtCreatedAt"))
df_cdc_files = (df_cdc_files
                    .withColumn("rn", functions.row_number().over(w))
                    .filter('rn=1')
                    .drop('rn')
                )

# Update table
(
    delta_table.alias("d")
    .merge(df_cdc_files.alias("cdc"), "cdc.idLobbyGame = d.idLobbyGame AND cdc.idPlayer = d.idPlayer")
    .whenMatchedDelete(condition = "cdc.Op = 'D'")
    .whenMatchedUpdateAll(condition = "cdc.Op ='U'")
    .whenNotMatchedInsertAll(condition = "cdc.Op = 'I'")
    .execute()
)
