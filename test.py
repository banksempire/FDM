import json
from datetime import datetime

import pymongo
from pymongo import MongoClient

import fdm


with open('setting.json', 'r', encoding='utf-8') as file:
    setting = json.loads(file.read())

dbSetting = setting['mongodb']
client = MongoClient(dbSetting['address'], dbSetting['port'])

pricedb = fdm.CleanData(client).price()

for res in pricedb.interface.rolling_query(10, startdate=datetime(2019, 1, 1), enddate=datetime(2019, 5, 1)):
    print(res.empty)

client.close()
