import os
from datetime import datetime
from pathlib import Path

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import pandas as pd
import sqlalchemy

CDC_FOLDER = "raw/cdc/tb_lobby_stats_player/"
CDC_QUERY = "select_recent_inserts.sql"

# Configure Azure credentials and blob client
account_url = "https://datalakebricks3478562.blob.core.windows.net"
default_credential = DefaultAzureCredential()

blob_service_client = BlobServiceClient(account_url, credential=default_credential)
container_name = "gc-bricks"
container_client = blob_service_client.get_container_client(container=container_name)

blob_list = container_client.list_blobs(name_starts_with=CDC_FOLDER)
file_dates = []
for blob in blob_list:
    file_date_str = blob.name.replace(CDC_FOLDER, "").split(".")[0]
    file_date = datetime.strptime(file_date_str, "%Y-%m-%d %H_%M_%S")
    file_dates.append(file_date)

conn = sqlalchemy.create_engine("sqlite:///../data/gc_database.db")
inspector = sqlalchemy.inspect(conn)

with open(CDC_QUERY, "r") as open_file:
    query = open_file.read()

query = query.format(cdc_date=f"'{max(file_dates)}'")
df = pd.read_sql(query, conn)

if(len(df)) > 0:

    date_series = pd.to_datetime(df['dtCreatedAt'])
    incremental_file_name = max(date_series).strftime("%Y-%m-%d %H_%M_%S")
    
    temp_csv_filename = (f'{Path(__file__).with_name(incremental_file_name)}.csv')
    df.to_csv(temp_csv_filename, index=False)

    blob_name = f"{CDC_FOLDER}{incremental_file_name}.csv"
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    print("\nUploading to Azure Storage as blob:\n\t" + blob_name)

    with open(file=temp_csv_filename, mode="rb") as data:
        blob_client.upload_blob(data=data, overwrite=True)
    
    if os.path.exists(temp_csv_filename):
        os.remove(temp_csv_filename)
    print("\nUpload complete")

else:
    print('\nNo additional entries to be uploaded')