import json
from datetime import datetime

import pymongo
from pymongo import MongoClient

import fdm


with open('setting.json', 'r', encoding='utf-8') as file:
    setting = json.loads(file.read())

dbSetting = setting['mongodb']
client = MongoClient(dbSetting['address'], dbSetting['port'])

db = fdm.Tushare(client)['daily_basic']
db.update()

client.close()
