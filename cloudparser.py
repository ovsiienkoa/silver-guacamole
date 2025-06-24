import time
import requests
import json
import datetime

with open("config.json", 'r', encoding="utf-8" ) as f:
    config = json.load(f)

with open("cases.json", 'r', encoding="utf-8" ) as f:
    case_list = json.load(f)

def get_json(start_point):
    params = {
            "query": "",
            "start": start_point,
            "count": 10,
            "search_descriptions": "commodity==1",
            "sort_column": "quantity",
            "sort_dir": "desc",
            "appid": 730,
            "category_730_Type[]": "tag_CSGO_Type_WeaponCase",
            "norender": 1
            }
    r = requests.get(
        "https://steamcommunity.com/market/search/render/",
        params=params)
    while r is None:
        time.sleep(PARSE_ERROR_DELAY)
        r = requests.get(
            "https://steamcommunity.com/market/search/render/",
            params=params)

    data = json.loads(r.text)
    return data

PARSE_TIME_DELAY = config["PARSE_TIME_DELAY"]
PARSE_ERROR_DELAY = config["PARSE_ERROR_DELAY"]

data = {"results": [1]}
start_point = 0

while len(data['results']) != 0 and len(case_list) != 0:

    data = get_json(start_point)
    start_point += 10

    batch = []
    for inst in data['results']:

        if inst['hash_name'] in case_list:
            case_list.remove(inst['hash_name'])
        else:
            continue

        timestamp = datetime.datetime.now(datetime.UTC)

        row = {'hash_name': inst['hash_name'],
               'sell_listings': inst['sell_listings'],
               'sell_price_text': inst['sell_price_text'],
               'sale_price_text': inst['sale_price_text'],
               'date': str(timestamp.date()),
               'hour': timestamp.hour,
               }
        batch.append(row)

    #sending batch to the cloud todo
    #if len(batch)==0: do not insert rows
    if len(batch) != 0:
        with open("cloudlike.csv", "a") as f:
            f.write(json.dumps(batch, indent=4))

    #sending batch to the cloud todo
    time.sleep(PARSE_TIME_DELAY)