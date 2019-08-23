import json

import pymongo
from pymongo import MongoClient

import fdm

with open('setting.json', 'r', encoding='utf-8') as file:
    setting = json.loads(file.read())

dbSetting = setting['mongodb']
client = MongoClient(dbSetting['address'], dbSetting['port'])

a = fdm.CleanData(client)
source = fdm.Tushare(client)
a.pricing().rebuild(source)

client.close()
