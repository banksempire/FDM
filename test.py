import json

import pymongo
from pymongo import MongoClient

import fdm

with open('setting.json', 'r', encoding='utf-8') as file:
    setting = json.loads(file.read())

dbSetting = setting['mongodb']
client = MongoClient(dbSetting['address'], dbSetting['port'])

pricedb = fdm.CleanData(client).pricing()

'''for df in pricedb.temp_dump():
    pricedb.interface.insert_many(df)'''

pricedb.interface.create_indexs(['code', 'date'])

client.close()
