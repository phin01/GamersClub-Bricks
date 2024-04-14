import sqlalchemy
import pandas as pd
import argparse

INCREMENTAL_LOAD_QUERY = "generate_random_lobby.sql"

parser = argparse.ArgumentParser()
parser.add_argument("--nPlayers", "-n", default=100)
args = parser.parse_args()

conn = sqlalchemy.create_engine("sqlite:///../data/gc_database.db")
inspector = sqlalchemy.inspect(conn)

with open(INCREMENTAL_LOAD_QUERY, "r") as open_file:
    query = open_file.read()

query = query.format(nPlayers=args.nPlayers)
df = pd.read_sql(query, conn)
df.to_sql("tb_lobby_stats_player", conn, if_exists="append", index=False)
