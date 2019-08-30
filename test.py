import json

import pymongo
from pymongo import MongoClient

import fdm

with open('setting.json', 'r', encoding='utf-8') as file:
    setting = json.loads(file.read())

dbSetting = setting['mongodb']
client = MongoClient(dbSetting['address'], dbSetting['port'])

print(client.abc.abc.full_name)


client.close()
