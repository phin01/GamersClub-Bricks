# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Azure Datalake 
# MAGIC
# MAGIC - All data in this project will be stored in blob storages
# MAGIC - Access is managed by a Service Principal with secrets stored in Azure Key Vault
# MAGIC - Key Vault secrets are made available in the notebooks through Databricks Secret Scope
# MAGIC - The project's container is mounted in Databricks for easier access throughout the notebooks

# COMMAND ----------

client_id = dbutils.secrets.get(scope='gcbricks-scope', key='gcbricksapp-client-id')
tenant_id = dbutils.secrets.get(scope='gcbricks-scope', key='gcbricksapp-tenant-id')
client_secret = dbutils.secrets.get(scope='gcbricks-scope', key='gcbricksapp-client-secret')

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# COMMAND ----------

dbutils.fs.mount(
    source = "abfss://gc-bricks@datalakebricks3478562.dfs.core.windows.net",
    mount_point="/mnt/gc-bricks",
    extra_configs=configs
)
