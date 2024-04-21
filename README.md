# Gamers Club Bricks
Sample Datalake project with Counter Strike matches data from [Gamer's Club](https://gamersclub.gg/) platform

Based on livestream project by [Téo Calvo](https://www.twitch.tv/collections/RfkhG2pJ7xY2TA) on Twitch

Data from [Gamers Club dataset](https://www.kaggle.com/datasets/gamersclub/brazilian-csgo-plataform-dataset-by-gamers-club) on Kaggle

## Tech stack
- Databricks
- PySpark
- DeltaLake
- Azure Blob Storage
- Azure Key Vault

## Process

### Raw layer
This layer in the blob storage will contain folders that represent each table in the source database
- tb_lobby_stats_player
- tb_medalha
- tb_players_medalha
- tb_players

Initially a full load is performed, to mirror the current status of the tables (raw/full_load.py)

Subsequent data loads should be incremental. In a real use case, this would be triggered by CDC in the source database, but in this project we'll simulate this process by 
1. Randomly picking matches from random players (raw/generate_random_lobby.sql)
2. Inserting these matches to the original database (raw/insert_random_matches.py)
3. Querying the updated database for recent inserts and uploading to datalake (raw/incremental_load.py)

### Bronze Layer

This layer will consolidate the original full load data and any subsequent incremental loads for all tables. From this point forward, all data transformation is done in Databricks
1. Define schema for all tables (bronze/schemas/*.json)
2. Create initial delta tables for each folder in raw layer (bronze/bronze_ingestion_full.py)
3. Update tb_lobby_stats_player table with incremental load if new files are found in raw cdc folder (bronze/bronze_ingestion_cdc.py)

##### Incremental Load
CDC files contain an additional column ('Op') that indicates if it's the result of an *Insert*, *Update* or *Delete* operation.
The CDC script checks the most recent entry for each key and tries to merge it with the existing delta table. If the entry is found, it'll *Update* or *Delete* based on the 'Op' column category. If the entry is not found and 'Op' category is *Insert*, it'll add the entry to the table

The script can be scheduled to run on any given frequency, since the merge/Op check prevents data duplication