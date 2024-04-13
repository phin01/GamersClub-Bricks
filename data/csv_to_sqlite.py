# The original files are distributed as csv files from a kaggle project
# ( https://www.kaggle.com/datasets/gamersclub/brazilian-csgo-plataform-dataset-by-gamers-club )

# We'll first convert these csv files into a sqlite3 database, to later on simulate ingestion from a SQL source

import sqlite3
import pandas as pd
from pathlib import Path

DB_NAME = 'gc_database.db'
Path(__file__).with_name(DB_NAME).touch()

def load_csv_to_table(filename, conn):
    df = pd.read_csv(f'{Path(__file__).with_name(filename)}.csv')
    df.to_sql(filename, conn, if_exists='append', index = False)


conn = sqlite3.connect(Path(__file__).with_name(DB_NAME))

with conn:
    c = conn.cursor()

    c.execute('''CREATE TABLE tb_medalha (
            idMedal int, 
            descMedal text,
            descTypeMedal text
            )''')

    c.execute('''CREATE TABLE tb_players (
            idPlayer int,
            flFacebook int,
            flTwitter int,
            flTwitch int,
            descCountry text,
            dtBirth text,
            dtRegistration text
            )''')

    c.execute('''CREATE TABLE tb_players_medalha (
            id int, 
            idPlayer int,
            idMedal int,
            dtCreatedAt text,
            dtExpiration text,
            dtRemove text,
            flActive int
            )''')

    c.execute('''CREATE TABLE tb_lobby_stats_player (
            idLobbyGame int,
            idPlayer int,
            idRoom int,
            qtKill int,
            qtAssist int,
            qtDeath int,
            qtHs int,
            qtBombeDefuse int,
            qtBombePlant int,
            qtTk int,
            qtTkAssist int,
            qt1Kill int,
            qt2Kill int,
            qt3Kill int,
            qt4Kill int,
            qt5Kill int,
            qtPlusKill int,
            qtFirstKill int,
            vlDamage int,
            qtHits int,
            qtShots int,
            qtLastAlive int,
            qtClutchWon int,
            qtRoundsPlayed int,
            descMapName text,
            vlLevel int,
            qtSurvived int,
            qtTrade int,
            qtFlashAssist int,
            qtHitHeadshot int,
            qtHitChest int,
            qtHitStomach int,
            qtHitLeftAtm int,
            qtHitRightArm int,
            qtHitLeftLeg int,
            qtHitRightLeg int,
            flWinner int,
            dtCreatedAt text
            )''')

    load_csv_to_table('tb_medalha', conn)
    load_csv_to_table('tb_players', conn)
    load_csv_to_table('tb_players_medalha', conn)
    load_csv_to_table('tb_lobby_stats_player', conn)