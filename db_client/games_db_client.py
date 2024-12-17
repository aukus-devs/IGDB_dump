import datetime
import MySQLdb
from MySQLdb.cursors import DictCursor
import logging
import os
import sys
from dotenv import load_dotenv
from contextlib import closing

load_dotenv()
MYSQL_LOGIN = os.getenv("MYSQL_LOGIN")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQLCONF = {
    "host": "127.0.0.1",
    "user": MYSQL_LOGIN,
    "password": MYSQL_PASSWORD,
    "db": "twitch_games_db",
    "port": 3306,
    "charset": "utf8mb4",
    "autocommit": True,
}


class GamesDatabaseClient:

    def __init__(self):
        self.connection = MySQLdb.connect(**MYSQLCONF)

    def safe_close(self):
        try:
            self.connection.close()
        except:
            pass

    def conn(self):
        if self.connection.open:
            try:
                self.connection.ping(True)
                return self.connection
            except:
                self.safe_close()
                self.connection = MySQLdb.connect(**MYSQLCONF)
                return self.connection
        else:
            self.safe_close()
            self.connection = MySQLdb.connect(**MYSQLCONF)
            return self.connection

    def insert_to_IGDB(self, game_id, name, cover_url, release_year,
                       platforms):
        with closing(self.conn().cursor(DictCursor)) as cursor:
            cursor.execute(
                """
                INSERT INTO igdb_games (game_id, name, cover_url, release_year, platforms)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (game_id, name, cover_url, release_year, platforms),
            )

    def insert_alternative_names_to_IGDB(self, name_id, game_id, name):
        with closing(self.conn().cursor(DictCursor)) as cursor:
            cursor.execute(
                """
                INSERT IGNORE INTO igdb_alternative_names (id, game_id, name)
                VALUES (%s, %s, %s)
                """,
                (name_id, game_id, name),
            )
