import sqlalchemy
import pandas as pd
import os
from pathlib import Path
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


# Configure Azure credentials and blob client
account_url = "https://datalakebricks3478562.blob.core.windows.net"
default_credential = DefaultAzureCredential()

blob_service_client = BlobServiceClient(account_url, credential=default_credential)
container_name = "gc-bricks"
container_client = blob_service_client.get_container_client(container=container_name)


# Upload a sqlite3 table to a blob as csv file
# Full load, overwriting file if it already exists in blob
def save_table_to_blob(table_name, sql_conn):
    temp_df = pd.read_sql_table(table_name, sql_conn)
    temp_csv_filename = (f'{Path(__file__).with_name(table_name)}.csv')
    temp_df.to_csv(temp_csv_filename, index=False)

    blob_name = f"raw/full-load/{table_name}/full-load.csv"
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    print("\nUploading to Azure Storage as blob:\n\t" + blob_name)

    with open(file=temp_csv_filename, mode="rb") as data:
        blob_client.upload_blob(data=data, overwrite=True)
    
    if os.path.exists(temp_csv_filename):
        os.remove(temp_csv_filename)
    print("\nUpload complete")


# List all tables in DB and save them to tbe blob storage
conn = sqlalchemy.create_engine("sqlite:///../data/gc_database.db")
inspector = sqlalchemy.inspect(conn)
tables = inspector.get_table_names()

for table in tables:
    save_table_to_blob(table, conn)


# Check if tables were succesfully saved
blob_list = container_client.list_blobs()
print("\Check existing files in Azure Storage container:\n")
for blob in blob_list:
    print("\t" + blob.name)