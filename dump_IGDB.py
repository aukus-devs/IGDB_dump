from db_client.db_client import DatabaseClient
from db_client.games_db_client import GamesDatabaseClient
import json
import os
import sys
from dotenv import load_dotenv
import logging
import time
import requests
from datetime import datetime

load_dotenv()
IGDB_CLIENT_ID = os.getenv("IGDB_CLIENT_ID")
igdb_session = CachedSession("igdb_cache", expire_after=timedelta(days=25), allowable_methods=['GET', 'POST'])
db = DatabaseClient()
games_db = GamesDatabaseClient()

playermoves = db.get_all_moves(limit=1000)

try:
    offset = 94500
    print("Get info with offset: " + str(offset))
    igdb_token = db.get_igdb_token()["igdb_token"]
    headers = {"Client-ID": IGDB_CLIENT_ID, "Authorization": "Bearer " + igdb_token}
    wrong_platforms = games_db.get_wrong_platforms()
    payload_fix = ""
    for platform in wrong_platforms:
        payload_fix += "|id=" + str(platform["platform_id"])
    payload = ('fields id,name,cover.image_id, first_release_date, platforms; limit 500; offset ' + str(offset) + ";").encode('utf-8')
    response = requests.post("https://api.igdb.com/v4/games", headers=headers, data=payload, timeout=2)
    if response.ok and "name" in response.text and len(response.text) > 2:
        games_json = json.loads(response.content.decode('utf-8'))
        for game in games_json:
            cover_url = "https://images.igdb.com/igdb/image/upload/t_cover_big/" + game["cover"]["image_id"] + ".jpg" if "cover" in game else ""
            release_date = game["first_release_date"] if "first_release_date" in game else 0
            release_year = 0
            if release_date != 0:
                release_year = int(datetime.utcfromtimestamp(release_date).strftime('%Y'))
            platforms = json.dumps(game["platforms"]) if "platforms" in game else "[]"
            games_db.insert_to_IGDB(game["id"], game["name"], cover_url, release_year, platforms)
    time.sleep(0.3)
except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    logging.error("Error: " + str(e) + ", offset: " + str(offset) + ", line: " + str(exc_tb.tb_lineno))

print(tr[1]) # force exit

for offset in range(0,320917,500):
    try:
        print("Get info with offset: " + str(offset))
        igdb_token = db.get_igdb_token()["igdb_token"]
        headers = {"Client-ID": IGDB_CLIENT_ID, "Authorization": "Bearer " + igdb_token}
        wrong_platforms = games_db.get_wrong_platforms()
        payload_fix = ""
        for platform in wrong_platforms:
            payload_fix += "|id=" + str(platform["platform_id"])
        payload = ('fields id,name,cover.image_id, first_release_date, platforms; limit 500; offset ' + str(offset) + ";").encode('utf-8')
        response = requests.post("https://api.igdb.com/v4/games", headers=headers, data=payload, timeout=2)
        if response.ok and "name" in response.text and len(response.text) > 2:
            games_json = json.loads(response.content.decode('utf-8'))
            for game in games_json:
                cover_url = "https://images.igdb.com/igdb/image/upload/t_cover_big/" + game["cover"]["image_id"] + ".jpg" if "cover" in game else ""
                release_date = game["first_release_date"] if "first_release_date" in game else 0
                release_year = 0
                if release_date != 0:
                    release_year = int(datetime.utcfromtimestamp(release_date).strftime('%Y'))
                platforms = json.dumps(game["platforms"]) if "platforms" in game else "[]"
                games_db.insert_to_IGDB(game["id"], game["name"], cover_url, release_year, platforms)
        time.sleep(0.3)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        logging.error("Error: " + str(e) + ", offset: " + str(offset) + ", line: " + str(exc_tb.tb_lineno))
