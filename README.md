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