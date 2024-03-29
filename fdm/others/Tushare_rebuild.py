import json

import pymongo
from pymongo import MongoClient

import fdm

with open('setting.json', 'r', encoding='utf-8') as file:
    setting = json.loads(file.read())

dbSetting = setting['mongodb']
client = MongoClient(dbSetting['address'], dbSetting['port'])

a = fdm.Tushare(client)
a.daily_adj().rebuild()
a.daily_basic().rebuild()
a.daily_price().rebuild()

client.close()
