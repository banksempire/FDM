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
df = pricedb.interface.query(
    code_list_or_str=['000001.SZ', '000002.SZ'], startdate=datetime(2000, 1, 1), enddate=datetime(2019, 6, 1), freq='Y', fillna='ffill')

print(df.pivot(index='date', columns='code', values='close').sort_index())

client.close()
