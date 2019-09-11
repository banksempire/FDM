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

#df = pricedb.ror(datetime(2018, 12, 31), freq='Y')
df = pricedb.query(
    startdate=datetime(2019, 5, 1), enddate=datetime(2019, 6, 1), freq='W', fillna='ffill')  #

print(df.pivot(index='date', columns='code', values='close').sort_index())

client.close()
